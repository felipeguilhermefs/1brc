local function ffnumber(str)
	local start = 1
	local negative = false
	if str:byte() == 45 then -- 45 = "-"
		start = 2
		negative = true
	end

	local split = str:find(".", 1, true)

	local base = 0
	for i = start, split - 1 do
		base = base * 10 + str:byte(i) - 48 -- 48 = "0"
	end

	-- Multiply by 10 to ensure integer
	-- Data has only 1 decimal digit
	local result = base * 10 + str:byte(split + 1) - 48
	if negative then
		result = result * -1
	end
	return result
end

local function work(filename, offset, limit)
	local file = assert(io.open(filename, "r"))

	-- Position reader at the correct offset
	file:seek("set", math.max(offset - 1, 0))
	local lines = file:lines()
	if offset > 0 and file:read(1) ~= "\n" then
		lines()
	end

	local records = {}
	for line in lines do
		local split = line:find(";", 1, true)
		local city = line:sub(1, split - 1)
		local temp = ffnumber(line:sub(split + 1))

		local record = records[city]
		if record then
			record[1] = math.min(record[1], temp)
			record[2] = math.max(record[2], temp)
			record[3] = record[3] + temp
			record[4] = record[4] + 1
		else
			records[city] = { temp, temp, temp, 1 }
		end

		-- Just work its batch
		if file:seek() >= limit then
			break
		end
	end
	file:close()

	local writePattern = "%s;%d;%d;%d;%d\n"
	for city, record in pairs(records) do
		io.write(writePattern:format(city, unpack(record)))
	end
end

local function fileSize(filename)
	local file = assert(io.open(filename, "r"))
	local filesize = file:seek("end")
	file:close()
	return filesize
end

local function fork(workAmount, workerCount)
	local batchSize = math.floor(workAmount / workerCount)
	local remainder = workAmount % workerCount
	local offset = 0
	local workers = {}
	local cmdPattern = "luajit 1brc.lua worker %d %d"
	for _ = 1, workerCount do
		-- Spread the remaining through the workers
		local limit = offset + batchSize + math.min(math.max(remainder, 0), 1)

		local cmd = cmdPattern:format(offset, limit)
		workers[#workers + 1] = assert(io.popen(cmd, "r"))

		offset = limit
		remainder = remainder - 1
	end
	return workers
end

local function join(workers)
	local statistics = {}
	local statsPattern = "(%S+);(%S+);(%S+);(%S+);(%S+)"
	for _, worker in pairs(workers) do
		for line in worker:lines() do
			local city, minT, maxT, sum, count = line:match(statsPattern)

			local stats = statistics[city]
			if stats then
				stats[1] = math.min(stats[1], minT)
				stats[2] = math.max(stats[2], maxT)
				stats[3] = stats[3] + sum
				stats[4] = stats[4] + count
			else
				statistics[city] = { minT, maxT, sum, count }
			end
		end

		worker:close()
	end
	return statistics
end

local function format(statistics)
	local outputPattern = "%s=%.1f/%.1f/%.1f"
	local output = {}
	for city, stats in pairs(statistics) do
		-- Divides by 10 to get back to original scale
		output[#output + 1] = outputPattern:format(city, stats[1] / 10, stats[3] / 10 / stats[4], stats[2] / 10)
	end

	return string.format("{%s}", table.concat(output, ","))
end

local function main()
	local filename = os.getenv("INPUT_FILE")
	if arg[1] == "worker" then
		work(filename, tonumber(arg[2]), tonumber(arg[3]))
	else
		local filesize = fileSize(filename)
		local parallelism = tonumber(os.getenv("PARALLELISM") or 4)
		local workers = fork(filesize, parallelism)
		local statistics = join(workers)
		local result = format(statistics)
		print(result)
	end
end

main()
