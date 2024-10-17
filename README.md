# 1BRC

My personal take on [1 Billion Row Challenge](https://1brc.dev/), but in [Lua](https://www.lua.org/).

## Run

- Generate the measurements file by following the original challenge [instructions](https://github.com/gunnarmorling/1brc?tab=readme-ov-file#running-the-challenge).
- Have [LuaJIT](https://luajit.org/index.html) 2.1 installed.
- Run `luajit 1brc.lua`

### Customize

Env vars:

- **PARALLELISM**=<number>, defaults to number of available CPU's
- **INPUT_FILE**=<filepath>, defaults to `measurements.txt`

## Optimizations

- Fork Join (via processes)
- Integer aggregation and serde (no floats)
- Each worker reads its chunk at once (no line by line)
- Table arrays for records instead of dictionaries
- Localize global functions for faster lookup
- Parse by iterating chars, no matches and only substring when needed
- Custom String to Number parser

## Result:

```
luajit 1brc.lua 57.66s user 5.52s system 1091% cpu 5.787 total
```

**~5.7s**


## Possible improvements

- Share memory between processes, so no need to serde intermediate results
- Real threads instead of processes
- Memory map the file
- Use C standard library with FFI

## Changelog

Read by line and aggregate:
	make 1brc  419.77s user 4.80s system 99% cpu 7:04.58 total
Read entire file:
	make 1brc  378.67s user 4.79s system 99% cpu 6:23.51 total
Small Lua specific optimizations:
	make 1brc  359.55s user 4.67s system 100% cpu 6:04.21 total
Avoid all globals:
	make 1brc  340.31s user 4.77s system 99% cpu 5:45.08 total
Format strings:
	make 1brc  338.68s user 4.90s system 99% cpu 5:43.65 total
Fork Join:
	make 1brc 844.45s user 8.30s system 1175% cpu 1:12.56 total
Fork Join (LuaJIT):
	make 1brc 796.99s user 7.17s system 1182% cpu 1:08.00 total
Find+Substr:
	make 1brc  642.97s user 6.90s system 1179% cpu 55.107 total
Fast and specific tonumber:
	make 1brc  353.31s user 6.58s system 1178% cpu 30.526 total
Workers read their chunk at once:
	make 1brc  285.57s user 5.84s system 1153% cpu 25.263 total
Use Find+Sub instead of Match:
	luajit     96.62s user 7.01s system 1048% cpu 9.886 total
Initialize hash size to minimize rehashes:
	luajit     93.47s user 6.43s system 1052% cpu 9.489 total
Stop GC and reduce amount of string created:
	luajit     57.66s user 5.52s system 1091% cpu 5.787 total
