local tnew = require("table.new")
local ffi = require("ffi")

ffi.cdef([[
typedef long int off_t;
typedef int pid_t;

int open(const char *path, int oflag, ...);
int close(int fildes);
void *mmap(void *addr, size_t len, int prot, int flags, int fd, off_t offset);
int munmap(void *addr, size_t len);
pid_t fork(void);
pid_t waitpid(pid_t pid, int *stat_loc, int options);
void _exit(int status);
int madvise(void *addr, size_t len, int advice);

typedef struct {
	int min;
	int max;
	int64_t sum;
	int count;
} Stats;

typedef struct {
    char name[100];
    int len;
    Stats stats;
} Entry;

typedef struct {
    int count;
    Entry entries[10000];
} WorkerResult;
]])

local O_RDONLY = 0
local PROT_READ = 1
local PROT_WRITE = 2
local MAP_PRIVATE = 2
local MAP_SHARED = 1
local MAP_ANON = 0x1000
local MADV_SEQUENTIAL = 2

collectgarbage("stop") -- Stop the GC do not need it for a quick run

local min = math.min
local max = math.max
local floor = math.floor
local fmt = string.format
local sort = table.sort
local concat = table.concat
local output = io.write

local MAX_STATIONS = 10000

local ASCII_LINEBREAK = 10
local ASCII_MINUS = 45
local ASCII_DOT = 46
local ASCII_ZERO = 48
local ASCII_SEMICOLON = 59

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

local function map_file(filename)
    local fd = ffi.C.open(filename, O_RDONLY)
    if fd < 0 then error("could not open file") end
    
    local size = filesize(filename)

    local ptr = ffi.C.mmap(nil, size, PROT_READ, MAP_SHARED, fd, 0)
    if ptr == ffi.cast("void*", -1) then
        ffi.C.close(fd)
        error("mmap failed")
    end
    ffi.C.close(fd)
    return ffi.cast("uint8_t*", ptr), size
end

local function create_shared_memory(size)
    local ptr = ffi.C.mmap(nil, size, PROT_READ + PROT_WRITE, MAP_ANON + MAP_SHARED, -1, 0)
    if ptr == ffi.cast("void*", -1) then
        error("mmap shared failed")
    end
    return ptr
end

local function work(ptr, start_offset, end_offset, worker_result, total_size)
    local statistics = tnew(0, MAX_STATIONS)
    local cur = ptr + start_offset
    local limit = ptr + end_offset
    local file_end = ptr + total_size

    -- Sync to first newline
    if start_offset > 0 then
        while cur < limit and cur[-1] ~= ASCII_LINEBREAK do
            cur = cur + 1
        end
    end

    while cur < limit do
        local start_station = cur
        while cur[0] ~= ASCII_SEMICOLON do
            cur = cur + 1
        end
        local station_len = cur - start_station
        local station = ffi.string(start_station, station_len)
        cur = cur + 1 -- Skip semicolon

        local negative = false
        if cur[0] == ASCII_MINUS then
            negative = true
            cur = cur + 1
        end

        local num = 0
        while cur[0] ~= ASCII_DOT do
            num = num * 10 + (cur[0] - ASCII_ZERO)
            cur = cur + 1
        end
        cur = cur + 1 -- Skip dot
        num = num * 10 + (cur[0] - ASCII_ZERO)
        cur = cur + 1 -- Skip decimal digit
        while cur < file_end and cur[0] ~= ASCII_LINEBREAK do
            cur = cur + 1
        end
        cur = cur + 1 -- Skip \n

        if negative then num = -num end

        local stats = statistics[station]
        if stats == nil then
            statistics[station] = ffi.new("Stats", num, num, num, 1)
        else
            if num < stats.min then stats.min = num end
            if num > stats.max then stats.max = num end
            stats.sum = stats.sum + num
            stats.count = stats.count + 1
        end
    end

    local i = 0
    for station, stats in pairs(statistics) do
        local entry = worker_result.entries[i]
        ffi.copy(entry.name, station)
        entry.len = #station
        entry.stats.min = stats.min
        entry.stats.max = stats.max
        entry.stats.sum = stats.sum
        entry.stats.count = stats.count
        i = i + 1
    end
    worker_result.count = i
end

local function fork_workers(ptr, size, n)
    local batchSize = floor(size / n)
    local shared_mem = create_shared_memory(ffi.sizeof("WorkerResult") * n)
    local results = ffi.cast("WorkerResult*", shared_mem)
    local pids = {}

    for i = 0, n - 1 do
        local offset = i * batchSize
        local limit = (i == n - 1) and size or (offset + batchSize)
        
        local pid = ffi.C.fork()
        if pid == 0 then
            -- In child
            work(ptr, offset, limit, results[i], size)
            ffi.C._exit(0)
        elseif pid > 0 then
            pids[#pids + 1] = pid
        else
            error("fork failed")
        end
    end
    
    -- Wait for all workers
    for _, pid in ipairs(pids) do
        ffi.C.waitpid(pid, nil, 0)
    end
    
    return results, n
end

local function aggregate_results(results, n)
    local statistics = tnew(0, MAX_STATIONS)
    for i = 0, n - 1 do
        local res = results[i]
        for j = 0, res.count - 1 do
            local entry = res.entries[j]
            local station = ffi.string(entry.name, entry.len)
            local stats = statistics[station]
            if stats == nil then
                statistics[station] = ffi.new("Stats", entry.stats.min, entry.stats.max, entry.stats.sum, entry.stats.count)
            else
                stats.min = min(stats.min, entry.stats.min)
                stats.max = max(stats.max, entry.stats.max)
                stats.sum = stats.sum + entry.stats.sum
                stats.count = stats.count + entry.stats.count
            end
        end
    end
    return statistics
end

local function formatJavaMap(statistics)
    local result = {}
    for station, stats in pairs(statistics) do
        -- Divides by 10 to get back to original scale
        local sum = tonumber(stats.sum)
        local avg = sum / 10 / stats.count
        local entry = fmt("%s=%.1f/%.1f/%.1f", station, tonumber(stats.min) / 10, avg, tonumber(stats.max) / 10)

        result[#result + 1] = entry
    end

    sort(result)

    return fmt("{%s}", concat(result, ","))
end

local function main(filename)
    local n = ncpu() * 3
    local ptr, size = map_file(filename)
    ffi.C.madvise(ptr, size, MADV_SEQUENTIAL)
    local results, num_workers = fork_workers(ptr, size, n)
    local statistics = aggregate_results(results, num_workers)
    output(formatJavaMap(statistics))
end

main(arg[1] or "measurements.txt")
