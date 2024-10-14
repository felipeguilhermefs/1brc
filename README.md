## Best Results:

### Lua

- Fork Join (via process)
- Integer Aggregation (no floats)
- Single Reads (no line by line)
- Table arrays over dictionaries
- Local lookups for frequently used functions (string.format, etc.)
- Substring over Match

#### Result:

```
lua 428.36s user 5.46s system 1158% cpu 37.449 total
```

**~37s**

### LuaJIT

- Fork Join (via process)
- Integer Aggregation (no floats)
- Custom String to Number
- Single Reads (no line by line)
- Table arrays over dictionaries

#### Result:

```
luajit 96.62s user 7.01s system 1048% cpu 9.886 total
```

**~10s**

### LuaJIT + FFI
