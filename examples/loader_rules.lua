-- vim: set ts=4 sw=4 sts=4 et:

ds_path = "##stub##"

print("in lua")

--- Sample N distinct integers from 1..M without replacement.
---@param M number -- size of the population (must be ≥ N)
---@param N number -- number of samples to draw
---@return number[] -- a table of length N containing the sampled indices
local function sampleIndices(M, N)
	assert(type(M) == "number" and type(N) == "number", "M and N must be numbers")
	assert(N <= M, "N must be ≤ M")
	-- initialize pool 1..M
	local pool = {}
	for i = 1, M do
		pool[i] = i
	end
	-- do N steps of Fisher–Yates: for i=1..N, swap pool[i] with a random element in [i..M]
	for i = 1, N do
		local j = math.random(i, M)
		pool[i], pool[j] = pool[j], pool[i]
	end
	-- take the first N entries as the sample
	local result = {}
	for i = 1, N do
		result[i] = pool[i]
	end
	return result
end

return {
	init_ctx = function(rank, world_size, _)
		print("init_ctx called with rank: " .. rank .. " and world_size: " .. world_size)

		local indices = {}
		for f in scan_dir(ds_path):iter() do
			if string.match(f, ".tar.utix$") then
				table.insert(indices, f)
			end
		end

		table.sort(indices)

		return { indices = indices }
	end,
	row_generator = function(ctx)
		print("row_generator")

		local fids = {}
		for _, idx_path in ipairs(ctx.indices) do
			local tar_path, _ = string.gsub(idx_path, ".utix$", "")
			local tar = g_loader:open_file(tar_path)
			print("file handle", tar, tar_path)
			table.insert(fids, tar)
		end

		for tar_idx, idx_path in ipairs(ctx.indices) do
			local utix = msgpack_unpacker(idx_path)
			local tar = fids[tar_idx]

			for row in utix:iter() do
				-- local s = string.format("row[%d] \"%s\" { ", row.iidx, row.str_idx)
				local base = row.offset

				local sampled_entries = sampleIndices(#row.keys, #row.keys // 2)

				for _, i in ipairs(sampled_entries) do
					local k = row.keys[i]
					local offset = base + row.offsets[i]
					local size = row.sizes[i]
					-- s = s .. string.format("%q: %X~%X ", k, offset, offset + size)

					g_loader:add_entry(tar, k, offset, size)

					-- NOTE: Uncomment below to check failure/error behavior
					g_loader.add_entry(tar, k .. "_fail", offset, size) -- Wrong syntax
				end
				-- s = s .. "}"
				-- print(s, #row.keys)

				g_loader:finish_row()
			end
		end
	end,
}
