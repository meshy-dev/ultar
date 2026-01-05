# ultar-dataloader

Fast async dataloader implemented in Zig with Lua scripting.

## Installation

### From wheel (recommended)

```bash
pip install ultar-dataloader
```

### Building from source

Requires Zig 0.15+.

```bash
# Build native extension
zig build python-bindings -Doptimize=ReleaseSafe

# Build wheel
python -m build --wheel --no-isolation python/

# Install
pip install python/dist/*.whl
```

## Usage

```python
from ultar_dataloader import DataLoader

# Define Lua loading script using module imports
LUA_SCRIPT = """
local loader = require("ultar.loader")
local utix = require("ultar.utix")

return {
    init_ctx = function(rank, world_size, config)
        -- Config is passed as 3rd argument from Python
        return {
            tar_path = config.tar_path,
            idx_path = config.idx_path,
            max_rows = tonumber(config.max_rows) or -1,
        }
    end,
    
    row_generator = function(ctx)
        local tar = loader:open_file(ctx.tar_path)
        local idx = utix.open(ctx.idx_path)

        local row_count = 0
        for row in idx:iter() do
            if ctx.max_rows > 0 and row_count >= ctx.max_rows then break end

            for i = 1, #row.keys do
                if row.sizes[i] > 0 then  -- Skip zero-size directory markers
                    loader:add_entry(tar, row.keys[i],
                        row.offset + row.offsets[i], row.sizes[i])
                end
            end
            loader:finish_row()
            row_count = row_count + 1
        end

        loader:close_file(tar)
    end,
}
"""

# Create dataloader with config dict
loader = DataLoader(
    src=LUA_SCRIPT,
    config={
        "tar_path": "/path/to/data.tar",
        "idx_path": "/path/to/data.tar.utix",
        "max_rows": "100",  # Values are strings, Lua converts as needed
    },
    rank=0,
    world_size=1,
)

# Iterate over rows
for row in loader:
    print(row.keys())
    data = row[".json"]  # Access by key name
    data = row[0]        # Or by index
```

## Features

- **High performance**: Uses io_uring (via libxev) for async I/O (~5 GB/s throughput)
- **Lua scripting**: Flexible data loading pipelines with full control
- **Config passing**: Pass Python dicts to Lua via `init_ctx(rank, world_size, config)`
- **Python native extension**: Proper GC integration, no ctypes issues
- **ABI3 compatible**: Works with Python 3.11+

## Lua Modules

Scripts use `require()` to import ultar modules:

| Module | Description |
|--------|-------------|
| `ultar.loader` | Async data loading interface |
| `ultar.utix` | Read `.utix` (msgpack) index files |
| `ultar.scandir` | Directory scanning utilities |

### ultar.loader

```lua
local loader = require("ultar.loader")

-- Open a file for reading (yields until complete)
local handle = loader:open_file("/path/to/file.tar")

-- Add entry to current row
loader:add_entry(handle, ".json", offset, size)

-- Finish row and make available to Python
loader:finish_row()

-- Close file
loader:close_file(handle)
```

### ultar.utix

```lua
local utix = require("ultar.utix")

-- Open a .utix index file
local idx = utix.open("/path/to/file.utix")

-- Iterate over rows
for row in idx:iter() do
    print(row.keys)    -- Array of entry keys
    print(row.offsets) -- Array of relative offsets
    print(row.sizes)   -- Array of sizes
    print(row.offset)  -- Base offset in tar file
end
```

## API Reference

### DataLoader

```python
DataLoader(
    src: str,                           # Lua script source code
    config: Mapping[str, str] | None,   # Config dict, passed to init_ctx as 3rd arg
    rank: int = 0,                      # Process rank for distributed loading
    world_size: int = 1,                # Total processes
    debug: bool = False,                # Enable debug logging
)
```

### LoadedRow

Dict-like access to loaded data:

```python
row.keys()      # List of entry keys
row.items()     # List of (key, bytes) tuples
row.to_dict()   # Dict mapping keys to bytes
row[key]        # Get bytes by key name
row[0]          # Get bytes by index
len(row)        # Number of entries
key in row      # Check if key exists
```

## CLI Tools

The package includes CLI tools for development:

```bash
# Initialize Lua LSP configuration in current directory
ultar-dataloader init-lsp

# Print path to shipped type stubs (for manual configuration)
ultar-dataloader types-path
```

The `init-lsp` command creates a `.luarc.json` file that configures [LuaLS](https://luals.github.io/) to recognize ultar modules with full type information:

```bash
$ ultar-dataloader init-lsp
Created /path/to/project/.luarc.json
Lua LSP will now recognize ultar modules from: /path/to/site-packages/ultar_dataloader/lua-types
```

## Development

```bash
# Build native extension (also copies to python/src for PYTHONPATH usage)
zig build python-bindings -Doptimize=ReleaseSafe

# Development install
pip install --no-build-isolation -e python/

# Run tests
python python/tests/test_dataloader.py
```
