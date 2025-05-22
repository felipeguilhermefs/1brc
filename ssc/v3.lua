--[[
-- V3:
--	- Read entire file
--	- Store statistics in an static array
--	- Local function lookup
--]]

local fmt = string.format
local sort = table.sort
local concat = table.concat

local MIN = 1
local MAX = 2
local SUM = 3
local COUNT = 4

local function read(filename)
	local file = assert(io.open(filename, "r"))
	local content = file:read("*all")
	file:close()
	return content
end

local function aggregate(content)
	local statistics = {}
	for station, measurement in content:gmatch("(%S+);(%S+)") do
		local temperature = tonumber(measurement)
		local stats = statistics[station]

		if stats == nil then
			statistics[station] = { temperature, temperature, temperature, 1 }
		else
			if stats[MIN] > temperature then
				stats[MIN] = temperature
			end
			if stats[MAX] < temperature then
				stats[MAX] = temperature
			end
			stats[SUM] = stats[SUM] + temperature
			stats[COUNT] = stats[COUNT] + 1
		end
	end
	return statistics
end

local function formatJavaMap(statistics)
	local result = {}
	for station, stats in pairs(statistics) do
		local avg = (stats[SUM] / stats[COUNT])
		local entry = fmt("%s=%.1f/%.1f/%.1f", station, stats[MIN], avg, stats[MAX])

		result[#result + 1] = entry
	end

	sort(result)

	return fmt("{%s}", concat(result, ","))
end

local function main(filename)
	local content = read(filename)
	local statistics = aggregate(content)
	print(formatJavaMap(statistics))
end

main("measurements.txt")
