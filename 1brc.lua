local max = math.max
local min = math.min
local number = tonumber
local fmt = string.format
local unpack = table.unpack or unpack

local function work(filename, offset, limit)
	local file = assert(io.open(filename, "r"))
	file:seek("set", max(offset - 1, 0))
	local nextLine = file:lines()
	if offset > 0 and file:read(1) ~= "\n" then
		nextLine()
	end

	local records = {}
	local record
	local city
	local temp
	for line in nextLine do
		city, temp = line:match("(%S+);(%S+)")
		temp = number(temp)

		record = records[city]
		if record then
			record[1] = min(record[1], temp)
			record[2] = max(record[2], temp)
			record[3] = record[3] + temp
			record[4] = record[4] + 1
		else
			records[city] = { temp, temp, temp, 1 }
		end

		if file:seek() >= limit then
			break
		end
	end
	file:close()

	for c, r in pairs(records) do
		io.write(fmt("%s;%.1f;%.1f;%.1f;%.1f\n", c, unpack(r)))
	end
end

local function calculateFileSize(filename)
	local file = assert(io.open(filename, "r"))
	local filesize = file:seek("end")
	file:close()
	return filesize
end

local function fork(workAmount, workerCount)
	local batchSize = math.floor(workAmount / workerCount)
	local remainder = workAmount % workerCount
	local offset = 0
	local limit
	local workers = {}
	for _ = 1, workerCount do
		-- Spread the remaining through the workers
		limit = offset + batchSize + min(max(remainder, 0), 1)

		local cmd = fmt("luajit 1brc.lua worker %d %d", offset, limit)
		workers[#workers + 1] = assert(io.popen(cmd, "r"))

		offset = limit
		remainder = remainder - 1
	end
	return workers
end

local function join(workers)
	local records = {}
	local record
	for _, worker in pairs(workers) do
		for line in worker:lines() do
			local city, minT, maxT, sum, count = line:match("(%S+);(%S+);(%S+);(%S+);(%S+)")

			record = records[city]
			if record then
				record[1] = min(record[1], number(minT))
				record[2] = max(record[2], number(maxT))
				record[3] = record[3] + number(sum)
				record[4] = record[4] + number(count)
			else
				records[city] = { number(minT), number(maxT), number(sum), number(count) }
			end
		end

		worker:close()
	end
	return records
end

local function format(statistics)
	local pattern = "%s=%.1f/%.1f/%.1f"

	local result = {}
	for city, stats in pairs(statistics) do
		result[#result + 1] = fmt(pattern, city, stats[1], stats[3] / stats[4], stats[2])
	end

	return fmt("{%s}", table.concat(result, ","))
end

local function main()
	local filename = os.getenv("INPUT_FILE")
	if arg[1] == "worker" then
		work(filename, number(arg[2]), number(arg[3]))
	else
		local filesize = calculateFileSize(filename)
		local parallelism = number(os.getenv("PARALLELISM") or 4)
		local workers = fork(filesize, parallelism)
		local statistics = join(workers)
		local result = format(statistics)
		print(result)
	end
end

main()
