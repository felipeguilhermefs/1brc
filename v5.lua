--[[
-- V5:
--	- Fork/Join 1 process per CPU
--	- Each worker reads its batch at once
--	- Store statistics in an static array
--	- Local function lookup
--]]

local min = math.min
local max = math.max
local floor = math.floor
local fmt = string.format
local sort = table.sort
local concat = table.concat
local unpack = table.unpack
local output = io.write

local MIN = 1
local MAX = 2
local SUM = 3
local COUNT = 4

local function work(filename, offset, limit)
	local file = assert(io.open(filename, "r"))

	-- Find position of first line in batch
	file:seek("set", max(offset - 1, 0))
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

	local statistics = {}
	for city, measurement in content:gmatch("(%S+);(%S+)") do
		local temperature = tonumber(measurement)

		local stats = statistics[city]
		if stats == nil then
			statistics[city] = { temperature, temperature, temperature, 1 }
		else
			stats[MIN] = min(stats[MIN], temperature)
			stats[MAX] = max(stats[MAX], temperature)
			stats[SUM] = stats[SUM] + temperature
			stats[COUNT] = stats[COUNT] + 1
		end
	end

	-- Send records back to master
	for city, stats in pairs(statistics) do
		output(fmt("%s;%.1f;%.1f;%.1f;%.1f\n", city, unpack(stats)))
	end
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

local function fork(workAmount, workerCount)
	local batchSize = floor(workAmount / workerCount)
	local remainder = workAmount % workerCount
	local offset = 0
	local workers = {}
	for _ = 1, workerCount do
		-- Spread the remaining through the workers
		local limit = offset + batchSize + min(max(remainder, 0), 1)

		local cmd = fmt("luajit v5 worker %d %d", offset, limit)
		workers[#workers + 1] = assert(io.popen(cmd, "r"))

		offset = limit
		remainder = remainder - 1
	end
	return workers
end

local function aggregate(statistics, worker)
	for line in worker:lines() do
		local city, minT, maxT, sum, count = line:match("(%S+);(%S+);(%S+);(%S+);(%S+)")

		local stats = statistics[city]

		if stats == nil then
			statistics[city] = { tonumber(minT), tonumber(maxT), tonumber(sum), tonumber(count) }
		else
			stats[MIN] = min(stats[MIN], tonumber(minT))
			stats[MAX] = max(stats[MAX], tonumber(maxT))
			stats[SUM] = stats[SUM] + tonumber(sum)
			stats[COUNT] = stats[COUNT] + tonumber(count)
		end
	end
end

local function join(workers)
	local statistics = {}
	for _, worker in pairs(workers) do
		aggregate(statistics, worker)
		worker:close()
	end
	return statistics
end

local function formatJavaMap(statistics)
	local result = {}
	for city, stats in pairs(statistics) do
		local avg = (stats[SUM] / stats[COUNT])
		local entry = fmt("%s=%.1f/%.1f/%.1f", city, stats[MIN], avg, stats[MAX])

		result[#result + 1] = entry
	end

	sort(result)

	return fmt("{%s}", concat(result, ","))
end

local function main(filename)
	if arg[1] == "worker" then
		local offset = tonumber(arg[2])
		local limit = tonumber(arg[3])
		work(filename, offset, limit)
	else
		local workers = fork(filesize(filename), ncpu())
		local statistics = join(workers)
		print(formatJavaMap(statistics))
	end
end

main("measurements.txt")
