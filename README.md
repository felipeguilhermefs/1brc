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

- Same as pure Lua
- Custom String to Number
- Table initialization to avoid rehashes

#### Result:

```
luajit 93.47s user 6.43s system 1052% cpu 8.589 total
```

**~8.5s**

### LuaJIT + FFI

- Same as LuaJIT
- C string to int/float convertion
- C struct instead of Lua table array

```
luajit 40.77s user 2.88s system 996% cpu 4.379 total
```

**~4.3s**
