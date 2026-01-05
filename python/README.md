# ultar-dataloader

Fast async dataloader implemented in Zig with Lua scripting.

## Installation

### From wheel (recommended)

```bash
pip install ultar-dataloader
```

### Building from source

Requires Zig 0.15+ (or use pixi which manages it).

```bash
# Using pixi (recommended)
pixi run build-wheel
pip install python/dist/*.whl

# Or manually with zig
zig build python-bindings -Doptimize=ReleaseFast
python -m build --wheel --no-isolation python/
pip install python/dist/*.whl
```

## Usage

```python
from ultar_dataloader import DataLoader

# Define Lua loading script
# File paths are passed via the config dict and accessed as g_config in Lua
LUA_SCRIPT = """
return {
    init_ctx = function(rank, world_size)
        return {}
    end,
    row_generator = function(ctx)
        -- Access config values passed from Python
        local tar = g_loader:open_file(g_config.tar_path)
        local utix = msgpack_unpacker(g_config.idx_path)
        local max_rows = tonumber(g_config.max_rows) or -1

        local row_count = 0
        for row in utix:iter() do
            if max_rows > 0 and row_count >= max_rows then break end

            for i = 1, #row.keys do
                if row.sizes[i] > 0 then  -- Skip zero-size directory markers
                    g_loader:add_entry(tar, row.keys[i],
                        row.offset + row.offsets[i], row.sizes[i])
                end
            end
            g_loader:finish_row()
            row_count = row_count + 1
        end

        g_loader:close_file(tar)
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

- **High performance**: Uses io_uring (via libxev) for async I/O (~2-4 GB/s throughput)
- **Lua scripting**: Flexible data loading pipelines with full control
- **Config passing**: Pass Python dicts to Lua via `g_config` global table
- **Python native extension**: Proper GC integration, no ctypes issues
- **ABI3 compatible**: Works with Python 3.11+

## API Reference

### DataLoader

```python
DataLoader(
    src: str,                           # Lua script source code
    config: Mapping[str, str] | None,   # Config dict, available as g_config in Lua
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

## Development

```bash
# Install pixi environment
pixi install

# Build native extension
pixi run build-native

# Development install
pixi run dev-install

# Run tests
pixi run python python/tests/test_dataloader.py
```
