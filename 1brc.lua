local tnew = require("table.new")
local ffi = require("ffi")

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

local function work(filename, offset, limit, initCities)
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

	local records = tnew(0, initCities)
	local cstart = 1
	local cend
	while cstart < #content do
		cend = content:find(";", cstart)
		local city = content:sub(cstart, cend - 1)
		cstart = cend + 1
		cend = content:find("\n", cstart)
		local temp = ffnumber(content:sub(cstart, cend - 1))
		cstart = cend + 1

		local record = records[city]
		if record then
			record[0] = math.min(record[0], temp)
			record[1] = math.max(record[1], temp)
			record[2] = record[2] + temp
			record[3] = record[3] + 1
		else
			records[city] = ffi.new("int[4]", { temp, temp, temp, 1 })
		end
	end

	local writePattern = "%s;%d;%d;%d;%d\n"
	for city, record in pairs(records) do
		io.write(writePattern:format(city, record[0], record[1], record[2], record[3]))
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

		workers[#workers + 1] = assert(io.popen(cmdPattern:format(offset, limit), "r"))

		offset = limit
		remainder = remainder - 1
	end
	return workers
end

local function join(workers, maxCities)
	local statistics = tnew(0, maxCities)
	local statsPattern = "(%S+);(%S+);(%S+);(%S+);(%S+)"
	for _, worker in pairs(workers) do
		for line in worker:lines() do
			local city, minT, maxT, sum, count = line:match(statsPattern)

			local stats = statistics[city]
			if stats then
				stats[0] = math.min(stats[0], tonumber(minT))
				stats[1] = math.max(stats[1], tonumber(maxT))
				stats[2] = stats[2] + tonumber(sum)
				stats[3] = stats[3] + tonumber(count)
			else
				statistics[city] = ffi.new("int[4]", { tonumber(minT), tonumber(maxT), tonumber(sum), tonumber(count) })
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
		output[#output + 1] = outputPattern:format(city, stats[0] / 10, stats[2] / 10 / stats[3], stats[1] / 10)
	end

	return string.format("{%s}", table.concat(output, ","))
end

local function main()
	local filename = os.getenv("INPUT_FILE")
	local parallelism = tonumber(os.getenv("PARALLELISM") or 4)
	local maxCities = 10000 -- at Most 10000 cities, from rules and limits
	if arg[1] == "worker" then
		work(filename, tonumber(arg[2]), tonumber(arg[3]), math.floor(maxCities / parallelism))
	else
		local filesize = fileSize(filename)
		local workers = fork(filesize, parallelism)
		local statistics = join(workers, maxCities)
		io.write(format(statistics))
	end
end

main()
