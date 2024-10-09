local function read(filename)
	local file = assert(io.open(filename, "r"))
	local content = file:read("*all")
	file:close()
	return content
end

local function accumulate(content)
	local records = {}
	local temperature
	local record
	local number = tonumber
	for city, measurement in content:gmatch("(%S+);(%S+)") do
		temperature = number(measurement)
		record = records[city]
		if record then
			if record[1] > temperature then
				record[1] = temperature
			end
			if record[2] < temperature then
				record[2] = temperature
			end
			record[3] = record[3] + temperature
			record[4] = record[4] + 1
		else
			records[city] = { temperature, temperature, temperature, 1 }
		end
	end
	return records
end

local function join(statistics)
	local format = string.format
	local pattern = "%.2f/%.2f/%.2f"

	local result = {}
	for city, stats in pairs(statistics) do
		result[city] = format(pattern, stats[1], (stats[3] / stats[4]), stats[2])
	end
	return result
end

local function formatJavaMap(tbl)
	-- Results are in Java "Map.toString" format
	local format = string.format
	local pattern = "%s=%s"

	local result = {}
	for k, v in pairs(tbl) do
		result[#result + 1] = format(pattern, k, v)
	end
	return format("{%s}", table.concat(result, ","))
end

local function brc(filename)
	local content = read(filename)
	local statistics = accumulate(content)
	local result = join(statistics)
	print(formatJavaMap(result))
end

brc(arg[1])
