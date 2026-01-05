---Ultar Dataloader Script
---
---This script defines how data is loaded from tar files.
---It exports two functions:
---  - init_ctx(rank, world_size, config) -> context
---  - row_generator(ctx) -> yields rows via loader module
---
---The `config` table is passed from Python and contains string values.

local loader = require("ultar.loader")
local utix = require("ultar.utix")

---@class LoaderContext
---@field tar_path string Path to the tar file
---@field idx_path string Path to the .utix index file
---@field max_rows integer Maximum rows to load (-1 for unlimited)

return {
    ---Initialize the loader context.
    ---Called once when the DataLoader is created.
    ---@param rank integer Current process rank (0-indexed)
    ---@param world_size integer Total number of processes
    ---@param config table<string, string> Configuration from Python
    ---@return LoaderContext ctx Context passed to row_generator
    init_ctx = function(rank, world_size, config)
        return {
            tar_path = config.tar_path,
            idx_path = config.idx_path,
            max_rows = tonumber(config.max_rows) or -1,
        }
    end,

    ---Generate rows of data.
    ---Called as a coroutine - loader methods will yield.
    ---@param ctx LoaderContext Context from init_ctx
    row_generator = function(ctx)
        local tar = loader:open_file(ctx.tar_path)
        local idx = utix.open(ctx.idx_path)

        local row_count = 0
        for row in idx:iter() do
            if ctx.max_rows > 0 and row_count >= ctx.max_rows then
                break
            end

            local base = row.offset
            local entries_added = 0

            for i = 1, #row.keys do
                local key = row.keys[i]
                local size = row.sizes[i]
                -- Skip zero-size entries (directory markers like ._)
                if size > 0 then
                    local offset = base + row.offsets[i]
                    loader:add_entry(tar, key, offset, size)
                    entries_added = entries_added + 1
                end
            end

            if entries_added > 0 then
                loader:finish_row()
                row_count = row_count + 1
            end
        end

        loader:close_file(tar)
    end,
}
