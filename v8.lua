--[[
-- V8:
--	- Fork/Join 2 process per CPU
--	- Each worker reads its batch at once
--	- Store statistics in an static array
--	- Local function lookup
--	- Avoid multiple rehashes
--	- Read by byte
--	- Use only integers
--	- Turn off GC
--	- C int parsing
--]]

local tnew = require("table.new")
local ffi = require("ffi")

ffi.cdef([[
int atoi(const char *nptr);
]])

collectgarbage("stop") -- Stop the GC do not need it for a quick run

local min = math.min
local max = math.max
local floor = math.floor
local chr = string.byte
local fmt = string.format
local substr = string.sub
local sort = table.sort
local concat = table.concat
local unpack = table.unpack
local output = io.write
local int = ffi.C.atoi

local MIN = 1
local MAX = 2
local SUM = 3
local COUNT = 4

local MAX_CITIES = 10000

local ASCII_LINEBREAK = 10
local ASCII_MINUS = 45
local ASCII_DOT = 46
local ASCII_ZERO = 48
local ASCII_SEMICOLON = 59

local function sfind(str, fromPos, untilChar)
	local cur = fromPos

	while cur < #str do
		if chr(str, cur) == untilChar then
			break
		end
		cur = cur + 1
	end

	return cur
end

local function ffnumber(str, sstart, send)
	local negative = false
	if chr(str, sstart) == ASCII_MINUS then
		sstart = sstart + 1
		negative = true
	end

	local dot = sstart
	while dot < send do
		if chr(str, dot) == ASCII_DOT then
			break
		end
		dot = dot + 1
	end

	local num = 0
	for i = sstart, dot - 1 do
		num = num * 10 + chr(str, i) - ASCII_ZERO
	end

	-- Multiply by 10 to ensure integer
	-- Data has only 1 decimal digit
	num = num * 10 + chr(str, dot + 1) - ASCII_ZERO
	if negative then
		num = num * -1
	end
	return num
end

local function work(filename, offset, limit)
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

	local statistics = tnew(0, MAX_CITIES)
	local cur = 1
	while cur < #content do
		local smcolon = sfind(content, cur, ASCII_SEMICOLON)
		local city = substr(content, cur, smcolon - 1)
		local brline = sfind(content, smcolon + 1, ASCII_LINEBREAK)
		local temperature = ffnumber(content, smcolon + 1, brline - 1)
		cur = brline + 1

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

		local cmd = fmt("luajit v8 worker %d %d", offset, limit)
		workers[#workers + 1] = assert(io.popen(cmd, "r"))

		offset = limit
		remainder = remainder - 1
	end
	return workers
end

local function aggregate(statistics, worker)
	for line in worker:lines() do
		local smcolon = sfind(line, 1, ASCII_SEMICOLON)
		local city = substr(line, 1, smcolon - 1)

		smcolon = sfind(line, smcolon + 1, ASCII_SEMICOLON)
		local minT = int(substr(line, smcolon - 1, ASCII_SEMICOLON))

		smcolon = sfind(line, smcolon + 1, ASCII_SEMICOLON)
		local maxT = int(substr(line, smcolon - 1, ASCII_SEMICOLON))

		smcolon = sfind(line, smcolon + 1, ASCII_SEMICOLON)
		local sum = int(substr(line, smcolon - 1, ASCII_SEMICOLON))

		local count = int(substr(line, smcolon + 1, #line))

		local stats = statistics[city]

		if stats == nil then
			statistics[city] = { minT, maxT, sum, count }
		else
			stats[MIN] = min(stats[MIN], minT)
			stats[MAX] = max(stats[MAX], maxT)
			stats[SUM] = stats[SUM] + sum
			stats[COUNT] = stats[COUNT] + count
		end
	end
end

local function join(workers)
	local statistics = tnew(0, MAX_CITIES)
	for _, worker in pairs(workers) do
		aggregate(statistics, worker)
		worker:close()
	end
	return statistics
end

local function formatJavaMap(statistics)
	local result = {}
	for city, stats in pairs(statistics) do
		-- Divides by 10 to get back to original scale
		local avg = stats[SUM] / 10 / stats[COUNT]
		local entry = fmt("%s=%.1f/%.1f/%.1f", city, stats[MIN] / 10, avg, stats[MAX] / 10)

		result[#result + 1] = entry
	end

	sort(result)

	return fmt("{%s}", concat(result, ","))
end

local function main(filename)
	local parallelism = ncpu() * 2 -- Keeps them busy
	if arg[1] == "worker" then
		local offset = int(arg[2])
		local limit = int(arg[3])
		work(filename, offset, limit)
	else
		local workers = fork(filesize(filename), parallelism)
		local statistics = join(workers)
		output(formatJavaMap(statistics))
	end
end

main("measurements.txt")
