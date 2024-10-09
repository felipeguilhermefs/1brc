local fmt = string.format
local min = math.min
local max = math.max
local number = tonumber

local function accumulate(filename)
	local records = {}

	local file = assert(io.open(filename, "rb"))
	local record
	for line in file:lines() do
		local semicolon = line:find(";", 1, true)
		local city = line:sub(1, semicolon - 1)
		local temperature = number(line:sub(semicolon + 1))
		record = records[city]
		if record then
			record[1] = min(record[1], temperature)
			record[2] = max(record[2], temperature)
			record[3] = record[3] + temperature
			record[4] = record[4] + 1
		else
			records[city] = { temperature, temperature, temperature, 1 }
		end
	end
	file:close()
	return records
end

local function format(statistics)
	local pattern = "%s=%.1f/%.1f/%.1f"

	local result = {}
	for city, stats in pairs(statistics) do
		result[#result + 1] = fmt(pattern, city, stats[1], stats[3] / stats[4], stats[2])
	end

	return fmt("{%s}", table.concat(result, ","))
end

local function brc(filename)
	local statistics = accumulate(filename)
	local result = format(statistics)
	print(result)
end

brc(arg[1])
