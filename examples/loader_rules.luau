print("in lua")

return {
  init_ctx = function(rank, world_size, c)
    print("init_ctx called with rank: " .. rank .. " and world_size: " .. world_size)

    return {}
  end,
  row_generator = function(ctx)
    print("row_generator")
  end,
}
