import ctypes

libdataloader = ctypes.CDLL("./zig-out/lib/libdataloader.so")
print(libdataloader)


class LuaLoaderSpec(ctypes.Structure):
    _fields_ = [
        ("shard_list", ctypes.POINTER(ctypes.c_char_p)),
        ("num_shards", ctypes.c_uint),
        ("src", ctypes.c_char_p),
        ("debug", ctypes.c_bool),
    ]


libdataloader.ultarCreateLuaLoader.argtypes = [LuaLoaderSpec]
libdataloader.ultarCreateLuaLoader.restype = ctypes.c_void_p

libdataloader.ultarDestroyLuaLoader.argtypes = [ctypes.c_void_p]

lua_src = """
--!strict

function ispositive(x : number) : string
    if x > 0 then
        return "yes"
    else
        return "no"
    end
end

local result : string
result = ispositive(1)
print("result is positive:", result)
"""

c_src = ctypes.c_char_p(lua_src.encode("utf-8"))

test_spec = LuaLoaderSpec()
test_spec.shard_list = ctypes.POINTER(ctypes.c_char_p)()
test_spec.num_shards = 0
test_spec.src = c_src
test_spec.debug = True

loader = libdataloader.ultarCreateLuaLoader(test_spec)
libdataloader.ultarDestroyLuaLoader(loader)
