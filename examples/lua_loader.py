import ctypes
from pathlib import Path
import sys
import re
from tqdm import tqdm


libdataloader = ctypes.CDLL("./zig-out/lib/libdataloader.so")
print(sys.argv)


class LuaLoaderSpec(ctypes.Structure):
    _fields_ = [
        ("src", ctypes.c_char_p),
        ("shard_list", ctypes.POINTER(ctypes.c_char_p)),
        ("num_shards", ctypes.c_uint),
        ("rank", ctypes.c_uint),
        ("world_size", ctypes.c_uint),
        ("debug", ctypes.c_bool),
    ]


class LoadedRow(ctypes.Structure):
    _fields_ = [
        ("keys", ctypes.POINTER(ctypes.c_char_p)),
        ("data", ctypes.POINTER(ctypes.c_void_p)),
        ("sizes", ctypes.POINTER(ctypes.c_uint)),
        ("num_keys", ctypes.c_uint),
    ]


libdataloader.ultarCreateLuaLoader.argtypes = [LuaLoaderSpec]
libdataloader.ultarCreateLuaLoader.restype = ctypes.c_void_p

libdataloader.ultarDestroyLuaLoader.argtypes = [ctypes.c_void_p]

libdataloader.ultarNextRow.argtypes = [ctypes.c_void_p]
libdataloader.ultarNextRow.restype = ctypes.POINTER(LoadedRow)

libdataloader.ultarReclaimRow.argtypes = [ctypes.c_void_p, ctypes.POINTER(LoadedRow)]
libdataloader.ultarReclaimRow.restype = None


src = Path(__file__).parent / "loader_rules.luau"
src = src.open("r").read()
src = re.sub(r"##stub##", sys.argv[1], src)
print(src)
c_src = ctypes.c_char_p(src.encode("utf-8"))

test_spec = LuaLoaderSpec()
test_spec.src = c_src
test_spec.shard_list = ctypes.POINTER(ctypes.c_char_p)()
test_spec.num_shards = 0
test_spec.rank = 0
test_spec.world_size = 1
test_spec.debug = False

loader = libdataloader.ultarCreateLuaLoader(test_spec)
assert loader is not None

pgbar = tqdm()
while True:
    r = libdataloader.ultarNextRow(loader)
    if r:
        libdataloader.ultarReclaimRow(loader, r)
    else:
        break
    pgbar.update(1)

libdataloader.ultarDestroyLuaLoader(loader)
