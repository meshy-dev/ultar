ds_path = "##stub##"

print("in lua")

type Array<T> = { [number]: T }
type Row = { iidx: number, str_idx: string, offset: number, keys: Array<string>, offsets: Array<string>, sizes: Array<string> }

--- Sample N distinct integers from 1..M without replacement.
-- @param M  size of the population (must be ≥ N)
-- @param N  number of samples to draw
-- @return a table of length N containing the sampled indices
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
  init_ctx = function(rank, world_size, c)
    print("init_ctx called with rank: " .. rank .. " and world_size: " .. world_size)

    indices = {}
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

    fids = {}
    for i, idx_path in ipairs(ctx.indices) do
      tar_path, _ = string.gsub(idx_path, ".utix$", "")
      tar = g_loader:open_file(tar_path)
      print("file handle", tar, tar_path)
      table.insert(fids, tar)
    end

    for i, idx_path in ipairs(ctx.indices) do
      tar_path, _ = string.gsub(idx_path, ".utix$", "")
      utix = msgpack_unpacker(idx_path)
      tar = fids[i]

      for row: Row in utix:iter() do
        -- local s = string.format("row[%d] \"%s\" { ", row.iidx, row.str_idx)
        base = row.offset

        sampled_entries = sampleIndices(#row.keys, 40)

        for _, i in ipairs(sampled_entries) do
          k = row.keys[i]
          offset = base + row.offsets[i]
          size = row.sizes[i]
          -- s = s .. string.format("%q: %X~%X ", k, offset, offset + size)

          g_loader:add_entry(tar, k, offset, size)
        end
        -- s = s .. "}"
        -- print(s, #row.keys)

        g_loader:finish_row()
      end
    end
  end,
}
