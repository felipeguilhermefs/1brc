local function read(filename)
	local file = assert(io.open(filename, "r"))
	local content = file:read("*all")
	file:close()
	return content
end

local function aggregate(content)
	local records = {}
	for city, measurement in content:gmatch("(%S+);(%S+)") do
		local temperature = tonumber(measurement)
		local record = records[city]

		if record == nil then
			records[city] = { ["min"] = temperature, ["max"] = temperature, ["sum"] = temperature, ["count"] = 1 }
		else
			if record["min"] > temperature then
				record["min"] = temperature
			end
			if record["max"] < temperature then
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
	local content = read(filename)
	local records = aggregate(content)
	local statistics = join(records)
	print(formatJavaMap(statistics))
end

brc("measurements.txt")
