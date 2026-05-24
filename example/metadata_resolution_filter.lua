local loader = require("ultar.loader")
local utix = require("ultar.utix")

return {
	init_ctx = function(rank, world_size, config)
		return {
			tar_path = config.tar_path,
			idx_path = config.idx_path,
			max_rows = tonumber(config.max_rows) or 5,
			max_dimension = tonumber(config.max_dimension) or 1024,
		}
	end,

	row_generator = function(ctx)
		local tar = loader:open_file(ctx.tar_path)
		local idx = utix.open(ctx.idx_path)
		local emitted = 0

		for row in idx:iter() do
			local meta = row.metadata or {}
			local width = tonumber(meta[".width"] or 0)
			local height = tonumber(meta[".height"] or 0)

			-- Metadata lets us skip rows before reading image bytes from the tar.
			if width <= ctx.max_dimension and height <= ctx.max_dimension then
				for i = 1, #row.keys do
					if row.keys[i] == ".jpg" then
						loader:add_entry(tar, ".jpg", row.offset + row.offsets[i], row.sizes[i])
					end
				end
				loader:finish_row()
				emitted = emitted + 1
				if emitted >= ctx.max_rows then
					break
				end
			end
		end

		loader:close_file(tar)
	end,
}
