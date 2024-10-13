## Best Results:

### Lua

- Fork Join (via process)
- Integer Aggregation (no floats)
- Single Reads (no line by line)
- Table arrays over dictionaries
- Local lookups for frequently used functions (string.format, etc.)

#### Result:

```
lua  501.97s user 5.47s system 1168% cpu 43.411 total
```

**~43s**

### LuaJIT

### LuaJIT + FFI
