"""Exercise `lua_rt.luaDumpStack` through the real Python -> LuaDataLoader
path. Each scenario builds a DataLoader with a Lua spec that intentionally
errors; `printLuaErr` calls `luaDumpStack` and the trace is emitted on
stderr.

Run as a script: `PYTHONPATH=python/src python3 python/tests/test_lua_dump.py`
Or under pytest:  `pytest python/tests/test_lua_dump.py -s -vv`
"""

import sys

from ultar_dataloader._native import DataLoader


SYNTAX_ERROR_SPEC = "this is not valid lua syntax @@@"

RUNTIME_ERROR_SPEC = """
local label = "loader_with_runtime_error"
local config_blob = { tag = label, depth = 3, items = {1, 2, 3, 4, 5} }
return {
  init_ctx = function(rank, world_size, config)
    local locals_table = { rank = rank, world_size = world_size }
    -- Calling an undefined global from inside init_ctx unwinds via Lua
    -- runtime error and routes through printLuaErr -> luaDumpStack.
    return undefined_global(locals_table, config_blob)
  end,
  row_generator = function(ctx)
    return nil
  end,
}
"""

LOAD_RETURNS_NON_TABLE_SPEC = """
-- Loader script that returns a string instead of a table; the Zig side
-- rejects it after the chunk runs successfully.
return "not a table"
"""

# init_ctx recurses through a few Lua frames, then calls
# `ultar.debug.dump_stack()` from the innermost frame. Because the dump runs
# *during* a live call (not after `pcall` unwinds), the call-stack section
# of the trace shows every active Lua and C frame. After dumping we raise an
# error so the loader still bails out and the test exception assertion holds.
CALL_STACK_DEMO_SPEC = """
local dbg = require("ultar.debug")

local function inner(depth, label)
  if depth <= 0 then
    dbg.dump_stack()
    error("intentional error after dumping the live call stack")
  end
  return inner(depth - 1, label)
end

return {
  init_ctx = function(rank, world_size, config)
    local local_table = { rank = rank, world_size = world_size }
    inner(3, "from_init_ctx")
  end,
  row_generator = function(ctx)
    return nil
  end,
}
"""


def _expect_failure(label: str, src: str) -> None:
    sys.stderr.flush()
    sys.stdout.flush()
    print(f"\n=== {label} ===", flush=True)
    try:
        DataLoader(src=src, config={}, rank=0, world_size=1, debug=False)
    except Exception as e:
        print(f"  exception: {type(e).__name__}: {e}", flush=True)
        return
    raise AssertionError(f"{label}: DataLoader was expected to fail")


def test_syntax_error_dumps_stack():
    _expect_failure("syntax_error", SYNTAX_ERROR_SPEC)


def test_runtime_error_dumps_stack():
    _expect_failure("runtime_error_in_init_ctx", RUNTIME_ERROR_SPEC)


def test_load_returns_non_table_dumps_stack():
    _expect_failure("load_returns_non_table", LOAD_RETURNS_NON_TABLE_SPEC)


def test_call_stack_demo():
    _expect_failure("call_stack_demo_via_ultar_debug", CALL_STACK_DEMO_SPEC)


if __name__ == "__main__":
    test_syntax_error_dumps_stack()
    test_runtime_error_dumps_stack()
    test_load_returns_non_table_dumps_stack()
    test_call_stack_demo()
