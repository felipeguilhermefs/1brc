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
	local records = {}
	for city, measurement in content:gmatch("(%S+);(%S+)") do
		local temperature = tonumber(measurement)
		local record = records[city]

		if record == nil then
			records[city] = { temperature, temperature, temperature, 1 }
		else
			if record[MIN] > temperature then
				record[MIN] = temperature
			end
			if record[MAX] < temperature then
				record[MAX] = temperature
			end
			record[SUM] = record[SUM] + temperature
			record[COUNT] = record[COUNT] + 1
		end
	end
	return records
end

local function join(records)
	local statistics = {}
	for city, record in pairs(records) do
		local mean = (record[SUM] / record[COUNT])
		local stats = string.format("%s=%.1f/%.1f/%.1f", city, record[MIN], mean, record[MAX])

		statistics[#statistics + 1] = stats
	end
	return statistics
end

local function format(statistics)
	table.sort(statistics)

	return string.format("{%s}", table.concat(statistics, ","))
end

local function brc(filename)
	local content = read(filename)
	local records = aggregate(content)
	local statistics = join(records)
	print(format(statistics))
end

brc("measurements.txt")
