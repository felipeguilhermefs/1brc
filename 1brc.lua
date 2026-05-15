local tnew = require("table.new")
local ffi = require("ffi")

ffi.cdef([[

typedef int pid_t;
typedef long int off_t;

int open(const char *path, int oflag, ...);
int close(int fildes);

void *mmap(void *addr, size_t len, int prot, int flags, int fd, off_t offset);
int madvise(void *addr, size_t len, int advice);

pid_t fork(void);
pid_t waitpid(pid_t pid, int *stat_loc, int options);
void _exit(int status);

typedef struct {
	int min;
	int max;
	int sum;
	int count;
} Stats;

typedef struct {
	char name[100];
	int len;
	Stats stats;
} Station;

typedef struct {
	int count;
	Station stations[10000];
} Result;

]])

collectgarbage("stop") -- Stop the GC do not need it for a quick run

local min = math.min
local max = math.max
local floor = math.floor
local chr = string.byte
local fmt = string.format
local sort = table.sort
local concat = table.concat
local output = io.write

local ASCII_LINEBREAK = 10
local ASCII_CR = 13
local ASCII_MINUS = 45
local ASCII_DOT = 46
local ASCII_ZERO = 48
local ASCII_SEMICOLON = 59
local MADV_SEQUENTIAL = 2
local MAP_SHARED = 1
local MAP_ANON = 0x1000
local MAX_STATIONS = 10000
local O_RDONLY = 0
local PROT_READ = 1
local PROT_WRITE = 2

local function allocSharedMem(nElements)
	local size = ffi.sizeof("Result") * nElements

	local ptr = ffi.C.mmap(nil, size, PROT_READ + PROT_WRITE, MAP_ANON + MAP_SHARED, -1, 0)

	if ptr == ffi.cast("void*", -1) then
		error("mmap shared failed")
	end

	return ffi.cast("Result*", ptr)
end

local function filesize(filename)
	local file = assert(io.open(filename, "r"))
	local bytes = file:seek("end")
	file:close()
	return bytes
end

local function mapFile(filename)
	local fd = ffi.C.open(filename, O_RDONLY)
	if fd < 0 then
		error("could not open file")
	end
	local size = filesize(filename)

	local ptr = ffi.C.mmap(nil, size, PROT_READ, MAP_SHARED, fd, 0)
	if ptr == ffi.cast("void*", -1) then
		ffi.C.close(fd)
		error("mmap filed")
	end
	ffi.C.close(fd)

	local file = ffi.cast("uint8_t*", ptr)
	ffi.C.madvise(file, size, MADV_SEQUENTIAL)

	return file, size
end

local function ffnumber(ptr)
	local num = 0
	local sign = 1
	if ptr[0] == ASCII_MINUS then
		sign = -1
		ptr = ptr + 1
	end

	if ptr[1] == ASCII_DOT then -- d.dd
		num = (ptr[0] - ASCII_ZERO) * 10 + (ptr[2] - ASCII_ZERO)
		ptr = ptr + 4
	else -- dd.d
		num = (ptr[0] - ASCII_ZERO) * 100 + (ptr[1] - ASCII_ZERO) * 10 + (ptr[3] - ASCII_ZERO)
		ptr = ptr + 5
	end
	num = num * sign
	return ptr, num
end

local function work(file, batchStart, batchEnd, size, result)
	local statistics = tnew(0, MAX_STATIONS)
	local offset = file + batchStart
	local limit = file + batchEnd
	local fileEnd = file + size

	-- Find position of first line in batch
	if batchStart > 0 then
		while offset < limit and offset[-1] ~= ASCII_LINEBREAK do
			offset = offset + 1
		end
	end

	while offset < limit do
		local startStation = offset
		while offset[0] ~= ASCII_SEMICOLON do
			offset = offset + 1
		end

		local stationLen = offset - startStation
		local station = ffi.string(startStation, stationLen)

		offset = offset + 1 -- skip ;

		local newOffset, temperature = ffnumber(offset)
		offset = newOffset

		if offset < fileEnd and offset[-1] == ASCII_CR then
			offset = offset + 1
		end

		local stats = statistics[station]
		if stats == nil then
			statistics[station] = ffi.new("Stats", temperature, temperature, temperature, 1)
		else
			stats.min = min(stats.min, temperature)
			stats.max = max(stats.max, temperature)
			stats.sum = stats.sum + temperature
			stats.count = stats.count + 1
		end
	end

	-- Copy stats to shared memory
	local i = 0
	for station, stats in pairs(statistics) do
		local entry = result.stations[i]
		ffi.copy(entry.name, station)
		entry.len = #station
		entry.stats = stats
		i = i + 1
	end
	result.count = i
end

local function ncpu()
	local tool = assert(io.popen("sysctl -n hw.ncpu", "r"))
	local parallelism = assert(tool:read("*n"))
	tool:close()
	return parallelism
end

local function fork(file, size, nWorkers)
	local batchSize = floor(size / nWorkers)
	local workers = tnew(nWorkers, 0)
	local results = allocSharedMem(nWorkers)
	for i = 0, nWorkers - 1 do
		local offset = i * batchSize
		local limit = (i == nWorkers - 1) and size or (offset + batchSize)

		local pid = ffi.C.fork()
		if pid == 0 then
			-- In child process
			work(file, offset, limit, size, results[i])
			ffi.C._exit(0)
		elseif pid > 0 then
			workers[i + 1] = pid
		else
			error("fork failed")
		end
	end

	-- Wait for all workers
	for i = 1, nWorkers do
		ffi.C.waitpid(workers[i], nil, 0)
	end

	return results
end

local function join(results, nWorkers)
	local statistics = tnew(0, MAX_STATIONS)
	for i = 0, nWorkers - 1 do
		local result = results[i]

		for j = 0, result.count - 1 do
			local station = result.stations[j]
			local name = ffi.string(station.name, station.len)
			local stats = station.stats
			local agg = statistics[name]
			if agg == nil then
				statistics[name] = ffi.new("Stats", stats.min, stats.max, stats.sum, stats.count)
			else
				agg.min = min(agg.min, stats.min)
				agg.max = max(agg.max, stats.max)
				agg.sum = agg.sum + stats.sum
				agg.count = agg.count + stats.count
			end
		end
	end
	return statistics
end

local function formatJavaMap(statistics)
	local result = {}
	for station, stats in pairs(statistics) do
		-- Divides by 10 to get back to original scale
		local avg = stats.sum / 10 / stats.count
		local entry = fmt("%s=%.1f/%.1f/%.1f", station, stats.min / 10, avg, stats.max / 10)

		result[#result + 1] = entry
	end

	sort(result)

	return fmt("{%s}", concat(result, ","))
end

local function main(filename)
	local nWorkers = ncpu() * 3
	local file, size = mapFile(filename)
	local results = fork(file, size, nWorkers)
	local statistics = join(results, nWorkers)
	output(formatJavaMap(statistics))
end

main("measurements.txt")
