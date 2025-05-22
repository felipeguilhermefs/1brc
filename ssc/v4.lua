--[[
-- v4:
--	- Fork/Join 1 process per CPU
--	- Each worker reads line by line
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

	-- Move reader to correct position
	file:seek("set", max(offset - 1, 0))
	local nextLine = file:lines()
	if offset > 0 and file:read(1) ~= "\n" then
		nextLine()
	end

	-- Aggregate
	local statistics = {}
	for line in nextLine do
		local station, measurement = line:match("(%S+);(%S+)")
		local temperature = tonumber(measurement)

		local stats = statistics[station]
		if stats then
			stats[MIN] = min(stats[MIN], temperature)
			stats[MAX] = max(stats[MAX], temperature)
			stats[SUM] = stats[SUM] + temperature
			stats[COUNT] = stats[COUNT] + 1
		else
			statistics[station] = { temperature, temperature, temperature, 1 }
		end

		-- Stop aggregating when slice of work is done
		if file:seek() >= limit then
			break
		end
	end
	file:close()

	-- Send records back to master
	for station, stats in pairs(statistics) do
		output(fmt("%s;%.1f;%.1f;%.1f;%.1f\n", station, unpack(stats)))
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

local function fork(filesize, ncpu)
	local batchSize = floor(filesize / ncpu)
	local remainder = filesize % ncpu
	local offset = 0
	local workers = {}
	for _ = 1, ncpu do
		-- Spread the remaining through the workers
		local limit = offset + batchSize + min(max(remainder, 0), 1)

		local cmd = fmt("luajit v4 worker %d %d", offset, limit)
		workers[#workers + 1] = assert(io.popen(cmd, "r"))

		offset = limit
		remainder = remainder - 1
	end
	return workers
end

local function aggregate(statistics, worker)
	for line in worker:lines() do
		local station, minT, maxT, sum, count = line:match("(%S+);(%S+);(%S+);(%S+);(%S+)")

		local stats = statistics[station]

		if stats == nil then
			statistics[station] = { tonumber(minT), tonumber(maxT), tonumber(sum), tonumber(count) }
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
	for station, stats in pairs(statistics) do
		local avg = (stats[SUM] / stats[COUNT])
		local entry = fmt("%s=%.1f/%.1f/%.1f", station, stats[MIN], avg, stats[MAX])

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
