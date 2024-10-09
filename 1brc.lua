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

local function format(statistics)
	local fmt = string.format
	local pattern = "%s=%.1f/%.1f/%.1f"

	local result = {}
	for city, stats in pairs(statistics) do
		result[#result + 1] = fmt(pattern, city, stats[1], stats[3] / stats[4], stats[2])
	end

	return fmt("{%s}", table.concat(result, ","))
end

local function brc(filename)
	local clock = os.clock
	print("start", clock())
	local content = read(filename)
	print("read", clock())
	local statistics = accumulate(content)
	print("accumulate", clock())
	local result = format(statistics)
	print("format", clock())
	print(result)
end

brc(arg[1])
