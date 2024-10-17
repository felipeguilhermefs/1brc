local tnew = require("table.new")
local MAX_CITIES = 10000 -- at Most 10000 cities, from rules and limits
local INIT_CITIES = 512

collectgarbage("stop") -- Stop the GC (we will run it manually for better performances)

local chr = string.byte
local max = math.max
local min = math.min
local fmt = string.format
local substr = string.sub

local function ffnumber(str, sstart, send)
	local negative = false
	if chr(str, sstart) == 45 then -- 45 = "-"
		sstart = sstart + 1
		negative = true
	end

	local split = sstart
	while split < send do
		if chr(str, split) == 46 then -- 46 = "."
			break
		end
		split = split + 1
	end

	local base = 0
	for i = sstart, split - 1 do
		base = base * 10 + chr(str, i) - 48 -- 48 = "0"
	end

	-- Multiply by 10 to ensure integer
	-- Data has only 1 decimal digit
	local result = base * 10 + chr(str, split + 1) - 48
	if negative then
		result = result * -1
	end
	return result
end

local function work(filename, offset, limit)
	local file = assert(io.open(filename, "rb"))

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
		cend = cstart
		while cend < #content do
			if chr(content, cend) == 59 then -- 59 == ";"
				break
			end
			cend = cend + 1
		end
		local city = substr(content, cstart, cend - 1)
		cstart = cend + 1
		cend = cstart
		while cend < #content do
			if chr(content, cend) == 10 then -- 10 == "\n"
				break
			end
			cend = cend + 1
		end
		local temp = ffnumber(content, cstart, cend - 1)
		cstart = cend + 1

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

	local writePattern = "%s;%d;%d;%d;%d\n"
	for city, record in pairs(records) do
		io.write(fmt(writePattern, city, unpack(record)))
	end
end

local function fileSize(filename)
	local file = assert(io.open(filename, "rb"))
	local filesize = file:seek("end")
	file:close()
	return filesize
end

local function fork(workAmount, workerCount)
	local chunkSize = math.floor(workAmount / workerCount)
	local remainder = workAmount % workerCount
	local offset = 0
	local workers = tnew(workerCount, 0)
	local cmdPattern = "luajit 1brc.lua worker %d %d"
	for _ = 1, workerCount do
		-- Spread the remaining through the workers
		local limit = offset + chunkSize + min(max(remainder, 0), 1)

		workers[#workers + 1] = assert(io.popen(fmt(cmdPattern, offset, limit), "r"))

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

local function format(statistics)
	local outputPattern = "%s=%.1f/%.1f/%.1f"
	local output = tnew(MAX_CITIES, 0)
	for city, stats in pairs(statistics) do
		-- Divides by 10 to get back to original scale
		output[#output + 1] = fmt(outputPattern, city, stats[1] / 10, stats[3] / 10 / stats[4], stats[2] / 10)
	end

	table.sort(output)

	return fmt("{%s}", table.concat(output, ","))
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
	local filename = os.getenv("INPUT_FILE")
	local parallelism = ncpu() * 2 -- just keep them more busy for now
	if arg[1] == "worker" then
		work(filename, tonumber(arg[2]), tonumber(arg[3]))
	else
		local filesize = fileSize(filename)
		local workers = fork(filesize, parallelism)
		local statistics = join(workers)
		io.write(format(statistics))
	end
end

main()
