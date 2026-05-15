local tnew = require("table.new")
local ffi = require("ffi")

ffi.cdef([[

typedef int pid_t;
typedef long int off_t;

void *mmap(void *addr, size_t len, int prot, int flags, int fd, off_t offset);

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
local substr = string.sub
local sort = table.sort
local concat = table.concat
local output = io.write

local ASCII_LINEBREAK = 10
local ASCII_MINUS = 45
local ASCII_DOT = 46
local ASCII_ZERO = 48
local ASCII_SEMICOLON = 59
local MAP_SHARED = 1
local MAP_ANON = 0x1000
local MAX_STATIONS = 10000
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

local function ffind(str, fromPos, untilChar)
	local cur = fromPos

	while cur < #str do
		if chr(str, cur) == untilChar then
			break
		end
		cur = cur + 1
	end

	return cur
end

local function ffnumber(str, sstart, send)
	local negative = false
	if chr(str, sstart) == ASCII_MINUS then
		sstart = sstart + 1
		negative = true
	end

	local dot = sstart
	while dot < send do
		if chr(str, dot) == ASCII_DOT then
			break
		end
		dot = dot + 1
	end

	local num = 0
	for i = sstart, dot - 1 do
		num = num * 10 + chr(str, i) - ASCII_ZERO
	end

	-- Multiply by 10 to ensure integer
	-- Data has only 1 decimal digit
	num = num * 10 + chr(str, dot + 1) - ASCII_ZERO
	if negative then
		num = num * -1
	end
	return num
end

local function work(filename, result, offset, limit)
	local statistics = tnew(0, MAX_STATIONS)
	local file = assert(io.open(filename, "r"))

	-- Find position of first line in batch
	file:seek("set", math.max(offset - 1, 0))
	if offset > 0 and file:read(1) ~= "\n" then
		local _ = file:read("*l")
	end
	local startPos = file:seek()

	-- Find position of last line in batch
	file:seek("set", limit)
	if file:read(1) ~= "\n" then
		local _ = file:read("*l")
	end
	local endPos = file:seek()

	-- Read entire batch at once
	file:seek("set", startPos)
	local content = file:read(endPos - startPos)
	file:close()

	local cur = 1
	while cur < #content do
		local smcolon = ffind(content, cur, ASCII_SEMICOLON)
		local station = substr(content, cur, smcolon - 1)
		local brline = ffind(content, smcolon + 1, ASCII_LINEBREAK)
		local temperature = ffnumber(content, smcolon + 1, brline - 1)
		cur = brline + 1

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

local function filesize(filename)
	local file = assert(io.open(filename, "r"))
	local bytes = file:seek("end")
	file:close()
	return bytes
end

local function ncpu()
	local tool = assert(io.popen("sysctl -n hw.ncpu", "r"))
	local parallelism = assert(tool:read("*n"))
	tool:close()
	return parallelism
end

local function fork(filesize, nWorkers, filename)
	local batchSize = floor(filesize / nWorkers)
	local remainder = filesize % nWorkers
	local offset = 0
	local workers = tnew(nWorkers, 0)
	local results = allocSharedMem(nWorkers)
	for i = 1, nWorkers do
		-- Spread the remaining through the workers
		local limit = offset + batchSize + min(max(remainder, 0), 1)

		local pid = ffi.C.fork()
		if pid == 0 then
			-- In child process
			work(filename, results[i - 1], offset, limit) -- results[i-1] because its C offset
			ffi.C._exit(0)
		elseif pid > 0 then
			workers[i] = pid
		else
			error("fork failed")
		end

		offset = limit
		remainder = remainder - 1
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
	local results = fork(filesize(filename), nWorkers, filename)
	local statistics = join(results, nWorkers)
	output(formatJavaMap(statistics))
end

main("measurements.txt")
