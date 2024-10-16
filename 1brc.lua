local tnew = require("table.new")
local ffi = require("ffi")

ffi.cdef([[
int atoi(const char *nptr);
double atof(const char *nptr);
struct stats { int min, max, sum, count; };
]])

local C = ffi.C

local function work(filename, offset, limit, initCities)
	local file = assert(io.open(filename, "r"))

	-- Find position of first line in batch
	file:seek("set", math.max(offset - 1, 0))
	if offset > 0 and file:read(1) ~= "\n" then
		local _ = file:read("*l")
	end
	local startPos = file:seek()

	-- Find position of last line in batch
	file:seek("set", limit)
	if file:read(1) ~= "\n" then
		local _ = file:read("*l")
	end
	local endPos = file:seek()

	-- Read entire batch at once
	file:seek("set", startPos)
	local content = file:read(endPos - startPos)
	file:close()

	local records = tnew(0, initCities)
	local cstart = 1
	local cend
	while cstart < #content do
		cend = content:find(";", cstart)
		local city = content:sub(cstart, cend - 1)
		cstart = cend + 1
		cend = content:find("\n", cstart)
		local temp = C.atof(content:sub(cstart, cend - 1)) * 10
		cstart = cend + 1

		local record = records[city]
		if record then
			record.min = math.min(record.min, temp)
			record.max = math.max(record.max, temp)
			record.sum = record.sum + temp
			record.count = record.count + 1
		else
			records[city] = ffi.new("struct stats", { temp, temp, temp, 1 })
		end
	end

	local writePattern = "%s;%d;%d;%d;%d\n"
	for city, record in pairs(records) do
		io.write(writePattern:format(city, record.min, record.max, record.sum, record.count))
	end
end

local function fileSize(filename)
	local file = assert(io.open(filename, "r"))
	local filesize = file:seek("end")
	file:close()
	return filesize
end

local function fork(workAmount, workerCount)
	local batchSize = math.floor(workAmount / workerCount)
	local remainder = workAmount % workerCount
	local offset = 0
	local workers = {}
	local cmdPattern = "luajit 1brc.lua worker %d %d"
	for _ = 1, workerCount do
		-- Spread the remaining through the workers
		local limit = offset + batchSize + math.min(math.max(remainder, 0), 1)

		workers[#workers + 1] = assert(io.popen(cmdPattern:format(offset, limit), "r"))

		offset = limit
		remainder = remainder - 1
	end
	return workers
end

local function join(workers, maxCities)
	local statistics = tnew(0, maxCities)
	local statsPattern = "(%S+);(%S+);(%S+);(%S+);(%S+)"
	for _, worker in pairs(workers) do
		for line in worker:lines() do
			local city, minT, maxT, sum, count = line:match(statsPattern)

			local stats = statistics[city]
			if stats then
				stats.min = math.min(stats.min, C.atoi(minT))
				stats.max = math.max(stats.max, C.atoi(maxT))
				stats.sum = stats.sum + C.atoi(sum)
				stats.count = stats.count + C.atoi(count)
			else
				statistics[city] = ffi.new("struct stats", { C.atoi(minT), C.atoi(maxT), C.atoi(sum), C.atoi(count) })
			end
		end

		worker:close()
	end
	return statistics
end

local function format(statistics)
	local outputPattern = "%s=%.1f/%.1f/%.1f"
	local output = {}
	for city, stats in pairs(statistics) do
		-- Divides by 10 to get back to original scale
		output[#output + 1] = outputPattern:format(city, stats.min / 10, stats.sum / 10 / stats.count, stats.max / 10)
	end

	table.sort(output)

	return string.format("{%s}", table.concat(output, ","))
end

local function main()
	local filename = os.getenv("INPUT_FILE")
	local parallelism = C.atoi(os.getenv("PARALLELISM") or 4)
	local maxCities = 10000 -- at Most 10000 cities, from rules and limits
	if arg[1] == "worker" then
		work(filename, C.atoi(arg[2]), C.atoi(arg[3]), math.floor(maxCities / parallelism))
	else
		local filesize = fileSize(filename)
		local workers = fork(filesize, parallelism)
		local statistics = join(workers, maxCities)
		io.write(format(statistics))
	end
end

main()
