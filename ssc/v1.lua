--[[
--   V1: Most straighforward implementation
--]]
local function readAndAggregate(filename)
	local statistics = {}
	for line in io.lines(filename) do
		local station, measurement = line:match("(%S+);(%S+)")
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
			if temperature < stats["min"] then
				stats["min"] = temperature
			end

			if temperature > stats["max"] then
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
	local statistics = readAndAggregate(filename)
	print(formatJavaMap(statistics))
end

main("measurements.txt")
