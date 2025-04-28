import ctypes
from pathlib import Path

libdataloader = ctypes.CDLL("./zig-out/lib/libdataloader.so")
print(libdataloader)


class LuaLoaderSpec(ctypes.Structure):
    _fields_ = [
        ("src", ctypes.c_char_p),
        ("shard_list", ctypes.POINTER(ctypes.c_char_p)),
        ("num_shards", ctypes.c_uint),
        ("rank", ctypes.c_uint),
        ("world_size", ctypes.c_uint),
        ("debug", ctypes.c_bool),
    ]


libdataloader.ultarCreateLuaLoader.argtypes = [LuaLoaderSpec]
libdataloader.ultarCreateLuaLoader.restype = ctypes.c_void_p

libdataloader.ultarDestroyLuaLoader.argtypes = [ctypes.c_void_p]

src = Path(__file__).parent / "loader_rules.luau"
c_src = ctypes.c_char_p(src.open("rb").read())

test_spec = LuaLoaderSpec()
test_spec.src = c_src
test_spec.shard_list = ctypes.POINTER(ctypes.c_char_p)()
test_spec.num_shards = 0
test_spec.rank = 0
test_spec.world_size = 1
test_spec.debug = True

loader = libdataloader.ultarCreateLuaLoader(test_spec)
assert loader is not None
libdataloader.ultarDestroyLuaLoader(loader)
