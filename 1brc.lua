local tnew
if jit then
	tnew = require("table.new")
else
	tnew = function()
		return {}
	end
end

collectgarbage("stop") -- Stop the GC do not need it for a quick run

local chr = string.byte
local max = math.max
local min = math.min
local fmt = string.format
local substr = string.sub
local output = io.write

local MAX_CITIES = 10000 -- at Most 10000 cities, from rules and limits
local INIT_CITIES = 512
local FILENAME = os.getenv("INPUT_FILE") or "measurements.txt"

local function ffnumber(str, sstart, send)
	local negative = false
	if chr(str, sstart) == 45 then -- 45 = "-"
		sstart = sstart + 1
		negative = true
	end

	local dot = sstart
	while dot < send do
		if chr(str, dot) == 46 then -- 46 = "."
			break
		end
		dot = dot + 1
	end

	local num = 0
	for i = sstart, dot - 1 do
		num = num * 10 + chr(str, i) - 48 -- 48 = "0"
	end

	-- Multiply by 10 to ensure integer
	-- Data has only 1 decimal digit
	num = num * 10 + chr(str, dot + 1) - 48
	if negative then
		num = num * -1
	end
	return num
end

local function work(offset, limit)
	local file = assert(io.open(FILENAME, "rb"))

	-- Find position of first line in chunk
	file:seek("set", max(offset - 1, 0))
	if offset > 0 and file:read(1) ~= "\n" then
		local _ = file:read("*l")
	end
	local startPos = file:seek()

	-- Find position of last line in chunk
	file:seek("set", limit)
	if file:read(1) ~= "\n" then
		local _ = file:read("*l")
	end
	local endPos = file:seek()

	-- Read entire chunk at once
	file:seek("set", startPos)
	local content = file:read(endPos - startPos)
	file:close()

	local records = tnew(0, INIT_CITIES)
	local cstart = 1
	local cend
	while cstart < #content do
		-- Find ; position to extract the city
		cend = cstart
		while cend < #content do
			if chr(content, cend) == 59 then -- 59 == ";"
				break
			end
			cend = cend + 1
		end
		local city = substr(content, cstart, cend - 1)
		cstart = cend + 1

		-- Find \n to extract the measurement
		cend = cstart
		while cend < #content do
			if chr(content, cend) == 10 then -- 10 == "\n"
				break
			end
			cend = cend + 1
		end
		local temp = ffnumber(content, cstart, cend - 1)
		cstart = cend + 1

		-- Accumulate measurements
		local record = records[city]
		if record then
			record[1] = min(record[1], temp)
			record[2] = max(record[2], temp)
			record[3] = record[3] + temp
			record[4] = record[4] + 1
		else
			records[city] = { temp, temp, temp, 1 }
		end
	end

	for city, record in pairs(records) do
		output(city, ";", record[1], ";", record[2], ";", record[3], ";", record[4], "\n")
	end
end

local function fileSize()
	local file = assert(io.open(FILENAME, "rb"))
	local filesize = file:seek("end")
	file:close()
	return filesize
end

local function fork(workAmount, workerCount)
	local chunkSize = math.floor(workAmount / workerCount)
	local remainder = workAmount % workerCount
	local offset = 0
	local workers = tnew(workerCount, 0)
	local cmdPattern = "%s %s worker %d %d"
	for _ = 1, workerCount do
		-- Spread the remaining bytes between the workers
		local limit = offset + chunkSize + min(max(remainder, 0), 1)

		local cmd = fmt(cmdPattern, arg[-1], arg[0], offset, limit)
		workers[#workers + 1] = assert(io.popen(cmd, "r"))

		offset = limit
		remainder = remainder - 1
	end
	return workers
end

local function join(workers)
	local statistics = tnew(0, MAX_CITIES)
	local statsPattern = "(%S+);(%S+);(%S+);(%S+);(%S+)"
	for _, worker in pairs(workers) do
		for line in worker:lines() do
			local city, minT, maxT, sum, count = line:match(statsPattern)

			local stats = statistics[city]
			if stats then
				stats[1] = min(stats[1], minT)
				stats[2] = max(stats[2], maxT)
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

local function answer(statistics)
	local resultPattern = "%s=%.1f/%.1f/%.1f"
	local result = tnew(MAX_CITIES, 0)
	for city, stats in pairs(statistics) do
		-- Divides by 10 to get back to original scale
		result[#result + 1] = fmt(resultPattern, city, stats[1] / 10, stats[3] / 10 / stats[4], stats[2] / 10)
	end

	table.sort(result)

	output("{", table.concat(result, ","), "}")
end

local function ncpu()
	local parallelism = os.getenv("PARALLELISM")
	if parallelism then
		return tonumber(parallelism)
	end

	local tool = assert(io.popen("sysctl -n hw.ncpu", "r"))
	parallelism = assert(tool:read("*n"))
	tool:close()
	return parallelism
end

local function main()
	if arg[1] == "worker" then
		work(tonumber(arg[2]), tonumber(arg[3]))
	else
		local parallelism = ncpu() * 2 -- just keep them more busy for now
		local filesize = fileSize()
		local workers = fork(filesize, parallelism)
		local statistics = join(workers)
		answer(statistics)
	end
end

main()
