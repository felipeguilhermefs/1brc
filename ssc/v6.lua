--[[
-- V6:
--	- Fork/Join 1 process per CPU
--	- Each worker reads its batch at once
--	- Store statistics in an static array
--	- Local function lookup
--	- Avoid multiple rehashes
--	- Read by byte
--]]

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

local tnew = require("table.new")

local MIN = 1
local MAX = 2
local SUM = 3
local COUNT = 4
local MAX_CITIES = 10000
local ASCII_SEMICOLON = 59
local ASCII_LINEBREAK = 10

local function ffind(str, fromPos, untilChar)
	local cur = fromPos

	while cur < #str do
		if chr(str, cur) == untilChar then
			break
		end
		cur = cur + 1
	end

	return cur
end

local function work(filename, offset, limit)
	local file = assert(io.open(filename, "r"))

	-- Find position of first line in batch
	file:seek("set", max(offset - 1, 0))
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
		local smcolon = ffind(content, cur, ASCII_SEMICOLON)
		local station = substr(content, cur, smcolon - 1)
		local brline = ffind(content, smcolon + 1, ASCII_LINEBREAK)
		local temperature = tonumber(substr(content, smcolon + 1, brline - 1))
		cur = brline + 1

		local stats = statistics[station]
		if stats == nil then
			statistics[station] = { temperature, temperature, temperature, 1 }
		else
			stats[MIN] = min(stats[MIN], temperature)
			stats[MAX] = max(stats[MAX], temperature)
			stats[SUM] = stats[SUM] + temperature
			stats[COUNT] = stats[COUNT] + 1
		end
	end

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

		local cmd = fmt("luajit v6 worker %d %d", offset, limit)
		workers[#workers + 1] = assert(io.popen(cmd, "r"))

		offset = limit
		remainder = remainder - 1
	end
	return workers
end

local function aggregate(statistics, worker)
	for line in worker:lines() do
		local sstart = 1
		local send = ffind(line, sstart, ASCII_SEMICOLON)
		local station = substr(line, sstart, send - 1)

		sstart = send + 1
		send = ffind(line, sstart, ASCII_SEMICOLON)
		local minT = tonumber(substr(line, sstart, send - 1))

		sstart = send + 1
		send = ffind(line, sstart, ASCII_SEMICOLON)
		local maxT = tonumber(substr(line, sstart, send - 1))

		sstart = send + 1
		send = ffind(line, sstart, ASCII_SEMICOLON)
		local sum = tonumber(substr(line, sstart, send - 1))

		local count = tonumber(substr(line, send + 1, #line))

		local stats = statistics[station]

		if stats == nil then
			statistics[station] = { minT, maxT, sum, count }
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
	for station, stats in pairs(statistics) do
		local avg = stats[SUM] / stats[COUNT]
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
		output(formatJavaMap(statistics))
	end
end

main("measurements.txt")
