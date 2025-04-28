print("in lua")

return {
  init_ctx = function(rank, world_size, c)
    print("init_ctx called with rank: " .. rank .. " and world_size: " .. world_size)

    for filename in scan_dir("./"):iter() do
      print("Found file: " .. filename)
    end

    return {}
  end,
  row_generator = function(ctx)
    print("row_generator")
  end,
}
