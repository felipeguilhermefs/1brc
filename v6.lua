local tnew = require("table.new")

local MIN = 1
local MAX = 2
local SUM = 3
local COUNT = 4

local function work(filename, offset, limit, ncities)
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

	local records = tnew(0, ncities)
	local cstart = 1
	while cstart < #content do
		local cend = content:find(";", cstart)
		local city = content:sub(cstart, cend - 1)
		cstart = cend + 1
		cend = content:find("\n", cstart)
		local temp = tonumber(content:sub(cstart, cend - 1))
		cstart = cend + 1

		local record = records[city]
		if record == nil then
			records[city] = { temp, temp, temp, 1 }
		else
			record[MIN] = math.min(record[MIN], temp)
			record[MAX] = math.max(record[MAX], temp)
			record[SUM] = record[SUM] + temp
			record[COUNT] = record[COUNT] + 1
		end
	end

	-- Send records back to master
	for city, record in pairs(records) do
		io.write(string.format("%s;%.1f;%.1f;%.1f;%.1f\n", city, table.unpack(record)))
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
	local batchSize = math.floor(workAmount / workerCount)
	local remainder = workAmount % workerCount
	local offset = 0
	local workers = {}
	for _ = 1, workerCount do
		-- Spread the remaining through the workers
		local limit = offset + batchSize + math.min(math.max(remainder, 0), 1)

		local cmd = string.format("luajit v6 worker %d %d", offset, limit)
		workers[#workers + 1] = assert(io.popen(cmd, "r"))

		offset = limit
		remainder = remainder - 1
	end
	return workers
end

local function join(workers, ncities)
	local statistics = tnew(0, ncities)
	for _, worker in pairs(workers) do
		for line in worker:lines() do
			local city, minT, maxT, sum, count = line:match("(%S+);(%S+);(%S+);(%S+);(%S+)")

			local stats = statistics[city]
			if stats == nil then
				statistics[city] = { tonumber(minT), tonumber(maxT), tonumber(sum), tonumber(count) }
			else
				stats[MIN] = math.min(stats[MIN], tonumber(minT))
				stats[MAX] = math.max(stats[MAX], tonumber(maxT))
				stats[SUM] = stats[SUM] + tonumber(sum)
				stats[COUNT] = stats[COUNT] + tonumber(count)
			end
		end

		worker:close()
	end
	return statistics
end

local function format(statistics)
	local result = {}
	for city, stats in pairs(statistics) do
		result[#result + 1] =
			string.format("%s=%.1f/%.1f/%.1f", city, stats[MIN], stats[SUM] / stats[COUNT], stats[MAX])
	end

	table.sort(result)

	return string.format("{%s}", table.concat(result, ","))
end

local function main(filename)
	local ncities = 10000
	local parallelism = ncpu()
	if arg[1] == "worker" then
		local offset = tonumber(arg[2])
		local limit = tonumber(arg[3])
		work(filename, offset, limit, math.floor(ncities / parallelism))
	else
		local workers = fork(filesize(filename), parallelism)
		local statistics = join(workers, ncities)
		io.write(format(statistics))
	end
end

main("measurements.txt")
