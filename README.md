# Ultar

> Ultra-scale tar / webdataset

> [!NOTE]
> No gurantees whatsoever.
> We are just trying to make webdataset random-accessible,
> And make the indexing fast as frick

## Contribute / Develop

The project is built with `zig`. You should be able to build it with a `zig>=0.15` install.

```sh
zig build -Doptimize=ReleaseSafe
```

## Install

CI should provide a build for you. It should be fully static so no glibc requirements.

## Performance

Remote NFS storage (hosted by lambdalabs), scanning 32 tar files (~1.4GB each) with a single instance / process of `indexer`

```sh
        User time (seconds): 3.20
        System time (seconds): 72.28
        Percent of CPU this job got: 183%
        Elapsed (wall clock) time (h:mm:ss or m:ss): 0:41.09
        Maximum resident set size (kbytes): 10624
```

On this particular system a single instance of `indexer` saturates at around `10Gibps` with most of the time spent on Linux's NFS server.

It runs sligthly too fast for local NVMe storage so I didn't bother a instrumented test.

## Methodology

Simple single-process event loop based IO provided by `libxev` & thus wielding the full power of `IO_URING`.

Have I mentioned it's written with [zig](https://ziglang.org)

## Python Bindings

The `python/` directory contains ABI3-compatible Python bindings for the Lua dataloader.

```bash
# Build
zig build python-bindings -Doptimize=ReleaseSafe

# Build wheel
python -m build --wheel --no-isolation python/

# Install
pip install python/dist/*.whl
```

See `python/README.md` for usage details.

## Lua Scripting API

The dataloader uses Lua scripts for flexible data loading pipelines. Scripts use standard Lua `require()` to import modules:

```lua
local loader = require("ultar.loader")
local utix = require("ultar.utix")

return {
    init_ctx = function(rank, world_size, config)
        return {
            tar_path = config.tar_path,
            idx_path = config.idx_path,
        }
    end,

    row_generator = function(ctx)
        local tar = loader:open_file(ctx.tar_path)
        local idx = utix.open(ctx.idx_path)

        for row in idx:iter() do
            for i = 1, #row.keys do
                if row.sizes[i] > 0 then
                    loader:add_entry(tar, row.keys[i],
                        row.offset + row.offsets[i], row.sizes[i])
                end
            end
            loader:finish_row()
        end

        loader:close_file(tar)
    end,
}
```

### Available Modules

| Module | Description |
|--------|-------------|
| `ultar.loader` | Async data loading interface - open files, add entries, finish rows |
| `ultar.utix` | Read `.utix` (msgpack) index files |
| `ultar.scandir` | Directory scanning utilities |

### LSP Integration

We ship type stubs for [LuaLS](https://luals.github.io/) (the standard Lua language server). This provides:

- **Autocompletion** for all ultar modules
- **Hover documentation** with function signatures
- **Type checking** for parameters
- **Go to definition** support

#### Quick Setup (Recommended)

If you've installed `ultar-dataloader` via pip, use the CLI to set up LSP:

```bash
cd your-project/
ultar-dataloader init-lsp
```

This creates a `.luarc.json` pointing to the type stubs shipped with the package.

#### Manual Setup

For development or custom setups, add `.luarc.json` to your project root:

```json
{
  "$schema": "https://raw.githubusercontent.com/LuaLS/vscode-lua/master/setting/schema.json",
  "workspace.library": [
    "/path/to/ultar/lua-types"
  ],
  "runtime.version": "LuaJIT"
}
```

Or get the path programmatically:

```bash
ultar-dataloader types-path
```
