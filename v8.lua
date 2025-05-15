local tnew = require("table.new")
local ffi = require("ffi")

ffi.cdef([[
int atoi(const char *nptr);
]])

local C = ffi.C

collectgarbage("stop") -- Stop the GC do not need it for a quick run

local MIN = 1
local MAX = 2
local SUM = 3
local COUNT = 4

local chr = string.byte
local max = math.max
local min = math.min
local substr = string.sub

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

		local cmd = string.format("luajit v8 worker %d %d", offset, limit)
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
				statistics[city] = ffi.new("int[4]", { C.atoi(minT), C.atoi(maxT), C.atoi(sum), C.atoi(count) })
			else
				stats[MIN] = math.min(stats[MIN], C.atoi(minT))
				stats[MAX] = math.max(stats[MAX], C.atoi(maxT))
				stats[SUM] = stats[SUM] + C.atoi(sum)
				stats[COUNT] = stats[COUNT] + C.atoi(count)
			end
		end

		worker:close()
	end
	return statistics
end

local function format(statistics)
	local result = {}
	for city, stats in pairs(statistics) do
		-- Divides by 10 to get back to original scale
		result[#result + 1] =
			string.format("%s=%.1f/%.1f/%.1f", city, stats[MIN] / 10, stats[SUM] / 10 / stats[COUNT], stats[MAX] / 10)
	end

	table.sort(result)

	return string.format("{%s}", table.concat(result, ","))
end

local function main(filename)
	local ncities = 10000
	local parallelism = ncpu() * 2
	if arg[1] == "worker" then
		local offset = C.atoi(arg[2])
		local limit = C.atoi(arg[3])
		work(filename, offset, limit, math.floor(ncities / parallelism))
	else
		local workers = fork(filesize(filename), parallelism)
		local statistics = join(workers, ncities)
		io.write(format(statistics))
	end
end

main("measurements.txt")
