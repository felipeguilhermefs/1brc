local MIN = 1
local MAX = 2
local SUM = 3
local COUNT = 4

local function work(filename, offset, limit)
	local file = assert(io.open(filename, "r"))

	-- Move reader to correct position
	file:seek("set", math.max(offset - 1, 0))
	local nextLine = file:lines()
	if offset > 0 and file:read(1) ~= "\n" then
		nextLine()
	end

	-- Aggregate
	local records = {}
	for line in nextLine do
		local city, measurement = line:match("(%S+);(%S+)")
		local temperature = tonumber(measurement)

		local record = records[city]
		if record then
			record[MIN] = math.min(record[MIN], temperature)
			record[MAX] = math.max(record[MAX], temperature)
			record[SUM] = record[SUM] + temperature
			record[COUNT] = record[COUNT] + 1
		else
			records[city] = { temperature, temperature, temperature, 1 }
		end

		-- Stop aggregating when slice of work is done
		if file:seek() >= limit then
			break
		end
	end
	file:close()

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

		local cmd = string.format("luajit v4 worker %d %d", offset, limit)
		workers[#workers + 1] = assert(io.popen(cmd, "r"))

		offset = limit
		remainder = remainder - 1
	end
	return workers
end

local function join(workers)
	local statistics = {}
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
	local pattern = "%s=%.1f/%.1f/%.1f"

	local result = {}
	for city, stats in pairs(statistics) do
		result[#result + 1] = string.format(pattern, city, stats[1], stats[3] / stats[4], stats[2])
	end

	table.sort(result)

	return string.format("{%s}", table.concat(result, ","))
end

local function main(filename)
	if arg[1] == "worker" then
		local offset = tonumber(arg[2])
		local limit = tonumber(arg[3])
		work(filename, offset, limit)
	else
		local workers = fork(filesize(filename), ncpu())
		local statistics = join(workers)
		print(format(statistics))
	end
end

main("measurements.txt")
