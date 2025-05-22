--[[
-- V2: Read entire file
--]]
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
			statistics[station] = {
				["min"] = temperature,
				["max"] = temperature,
				["sum"] = temperature,
				["count"] = 1,
			}
		else
			if stats["min"] > temperature then
				stats["min"] = temperature
			end
			if stats["max"] < temperature then
				stats["max"] = temperature
			end
			stats["sum"] = stats["sum"] + temperature
			stats["count"] = stats["count"] + 1
		end
	end
	return statistics
end

local function formatJavaMap(statistics)
	local result = {}
	for station, stats in pairs(statistics) do
		local avg = (stats["sum"] / stats["count"])
		local entry = string.format("%s=%.1f/%.1f/%.1f", station, stats["min"], avg, stats["max"])

		table.insert(result, entry)
	end

	table.sort(result)

	return string.format("{%s}", table.concat(result, ","))
end

local function main(filename)
	local content = read(filename)
	local statistics = aggregate(content)
	print(formatJavaMap(statistics))
end

main("measurements.txt")
