local function aggregate(filename)
	local records = {}
	for line in io.lines(filename) do
		local city, measurement = line:match("(%S+);(%S+)")

		local temperature = tonumber(measurement)
		local record = records[city]

		if record == nil then
			records[city] = { ["min"] = temperature, ["max"] = temperature, ["sum"] = temperature, ["count"] = 1 }
		else
			if temperature < record["min"] then
				record["min"] = temperature
			end

			if temperature > record["max"] then
				record["max"] = temperature
			end

			record["sum"] = record["sum"] + temperature
			record["count"] = record["count"] + 1
		end
	end
	return records
end

local function join(records)
	local statistics = {}
	for city, record in pairs(records) do
		local mean = (record["sum"] / record["count"])
		local stats = string.format("%s=%.1f/%.1f/%.1f", city, record["min"], mean, record["max"])

		table.insert(statistics, stats)
	end
	return statistics
end

local function formatJavaMap(statistics)
	table.sort(statistics)

	return "{" .. table.concat(statistics, ",") .. "}"
end

local function brc(filename)
	local records = aggregate(filename)
	local statistics = join(records)
	print(formatJavaMap(statistics))
end

brc("measurements.txt")
