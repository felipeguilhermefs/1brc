local function read(filename)
	local file = assert(io.open(filename, "r"))
	local content = file:read("*all")
	file:close()
	return content
end

local MIN = 1
local MAX = 2
local SUM = 3
local COUNT = 4

local function accumulate(content)
	local records = {}
	local temperature
	local record
	for city, measurement in content:gmatch("(%S+);(%S+)") do
		temperature = tonumber(measurement)
		record = records[city]
		if record then
			if record[MIN] > temperature then
				record[MIN] = temperature
			end
			if record[MAX] < temperature then
				record[MAX] = temperature
			end
			record[SUM] = record[SUM] + temperature
			record[COUNT] = record[COUNT] + 1
		else
			records[city] = { temperature, temperature, temperature, 1 }
		end
	end
	return records
end

local function join(statistics)
	local result = {}
	for city, stats in pairs(statistics) do
		result[city] = stats[MIN] .. "/" .. (stats[SUM] / stats[COUNT]) .. "/" .. stats[MAX]
	end
	return result
end

local function formatJavaMap(tbl)
	-- Results are in Java "Map.toString" format
	local result = "{"
	for k, v in pairs(tbl) do
		result = result .. k .. "=" .. v .. ","
	end
	-- Remove leading commas from the result
	if result ~= "" then
		result = result:sub(1, result:len() - 1)
	end
	return result .. "}"
end

local function brc(filename)
	local content = read(filename)
	local statistics = accumulate(content)
	local result = join(statistics)
	print(formatJavaMap(result))
end

brc(arg[1])
