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
- Create hash maps already at max size to avoid rehashes
- No GC

## Result:

MacBook Pro M2 Max, 12 cores, 64GB RAM.

```
luajit 1brc.lua 26.81s user 2.42s system 1027% cpu 2.846 total
```

**~2.4s**


## Possible improvements

- Share memory between processes, so no need to serde intermediate results
- Real threads instead of processes
- Bitwise ops / bitmasks to avoid parsing
- Use GPU
- Better distribute the workload between processes

