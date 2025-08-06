const std = @import("std");
const zlua = @import("zlua");
const Lua = @import("zlua").Lua;
const msgpack = @import("msgpack.zig");

const logger = std.log.scoped(.lua_rt);

const ScanCtx = struct {
    dir: std.fs.Dir,
    iter: std.fs.Dir.Iterator,

    const g_new_ctx = "scan_dir";
    const f_iter = "iter";
    const meta_table = "ScanCtxMT";

    pub fn luauDetor(data: *anyopaque) void {
        const ctx: *ScanCtx = @ptrFromInt(@intFromPtr(data));
        ctx.dir.close();
    }

    pub fn luaDetor(lua: *Lua) !c_int {
        const ctx = try lua.toUserdata(ScanCtx, 1);
        ctx.dir.close();
        return 0;
    }
};

pub fn luaDumpStack(lua: *Lua) void {
    // var level: i32 = 0;
    // while (lua.getStack(level) catch null) |debug_info| {
    //     logger.info("[{}] file {s} line {}: {s} ", .{
    //         -level,
    //         debug_info.source,
    //         debug_info.current_line orelse -1,
    //         debug_info.name orelse "unknown",
    //     });
    //     level += 1;
    // }

    const top: usize = @intCast(lua.getTop());
    var buf: [1024]u8 = undefined;
    for (1..top + 1) |_i| {
        const i: i32 = @intCast(_i);
        const t = lua.typeOf(i);
        var value: []const u8 = undefined;
        switch (t) {
            .nil => {
                value = "nil";
            },
            .boolean => {
                value = if (lua.toBoolean(i)) "true" else "false";
            },
            .number => {
                const v = lua.toNumber(i) catch {
                    value = "unknown number";
                    break;
                };
                value = std.fmt.bufPrint(&buf, "{d}", .{v}) catch "fmt err";
            },
            .string => {
                value = lua.toString(i) catch "unknown string";
            },
            .table => {
                const len = lua.rawLen(i);
                value = std.fmt.bufPrint(&buf, "table(len={d})", .{len}) catch "fmt err";
            },
            .function => {
                value = "function";
            },
            .userdata => {
                value = "userdata";
            },
            else => {
                value = "unknown type";
            },
        }

        logger.info("Stack[{}]: {s}", .{ i, value });
    }
}

pub fn toUnsigned(lua: *Lua, idx: i32) !u32 {
    if (zlua.lang == .luau or zlua.lang == .lua52) {
        return lua.toUnsigned(idx);
    } else {
        const f = try lua.toNumber(idx);
        return @intFromFloat(f);
    }
}

pub fn pushUnsigned(lua: *Lua, v: u32) void {
    if (zlua.lang == .luau or zlua.lang == .lua52) {
        lua.pushUnsigned(v);
    } else {
        const f: f64 = @floatFromInt(v);
        lua.pushNumber(f);
    }
}

pub fn printLuaErr(lua: *Lua, err: zlua.Error) zlua.Error {
    switch (err) {
        error.LuaError, error.LuaRuntime, error.LuaSyntax => {
            const err_msg = lua.toString(-1) catch "unknown error";
            logger.err("{}Error:\n{s}", .{ err, err_msg });
        },
        else => {
            logger.err("Lua op returned {}", .{err});
        },
    }
    luaDumpStack(lua);
    std.debug.dumpCurrentStackTrace(null);
    return err;
}

fn newScanCtx(lua: *Lua) !i32 {
    const path = lua.toString(1) catch |err| return printLuaErr(lua, err);

    const ctx = switch (zlua.lang) {
        // Other runtimes use __gc metamethod to clean up userdata
        .luau => lua.newUserdataDtor(ScanCtx, zlua.wrap(ScanCtx.luauDetor)),
        .lua54 => lua.newUserdata(ScanCtx, 0),
        else => lua.newUserdata(ScanCtx),
    };

    ctx.dir = std.fs.cwd().openDir(path, .{ .iterate = true }) catch |err| {
        logger.warn("Failed to open directory {s}: {}", .{ path, err });
        return zlua.Error.LuaFile;
    };
    ctx.iter = ctx.dir.iterate();

    _ = lua.getMetatableRegistry(ScanCtx.meta_table);
    lua.setMetatable(-2);

    return 1;
}

fn scanIterator(lua: *Lua) !i32 {
    const ctx = try lua.toUserdata(ScanCtx, Lua.upvalueIndex(1));
    const next_e = try ctx.iter.next();
    if (next_e) |entry| {
        var buf: [std.fs.max_path_bytes]u8 = undefined;
        const entry_name = try ctx.dir.realpath(entry.name, &buf);
        _ = lua.pushString(entry_name);
        return 1;
    } else {
        return 0;
    }
}

fn scanDir(lua: *Lua) i32 {
    _ = lua.checkUserdata(ScanCtx, 1, ScanCtx.meta_table);
    lua.pushValue(1); // [+p]
    lua.pushClosure(zlua.wrap(scanIterator), 1); // pop 1 & push fn
    return 1;
}

const MsgpackUnpacker = struct {
    const Self = @This();
    const BufferedReader = std.io.BufferedReader(65536, std.fs.File.Reader);

    pub const UnpackerImpl = struct {
        lua: *Lua,
        // Dynamic list of stack indices where each new table lives
        tables: std.ArrayList(i32),
        // Parallel stack tracking the next numeric key for array inserts
        arrayIdx: std.ArrayList(i32),

        pub fn init(lua: *Lua) UnpackerImpl {
            const alloc = lua.allocator();
            return .{
                .lua = lua,
                .tables = std.ArrayList(i32).init(alloc),
                .arrayIdx = std.ArrayList(i32).init(alloc),
            };
        }

        pub fn deinit(self: *UnpackerImpl) void {
            self.tables.deinit();
            self.arrayIdx.deinit();
        }

        pub fn reset(self: *UnpackerImpl) void {
            self.tables.clearRetainingCapacity();
            self.arrayIdx.clearRetainingCapacity();
        }

        pub fn unpackNil(self: *UnpackerImpl) !void {
            self.lua.pushNil(); // [+p]
            try self.assign(); // pop
        }

        pub fn unpackBool(self: *UnpackerImpl, v: bool) !void {
            self.lua.pushBoolean(v); // [+p]
            try self.assign(); // pop
        }

        pub fn unpackInt(self: *UnpackerImpl, v: i64) !void {
            if (v > std.math.maxInt(zlua.Integer)) {
                std.debug.assert(v < 2e14);
                self.lua.pushNumber(@floatFromInt(v));
            } else {
                self.lua.pushInteger(@intCast(v)); // [+p]
            }
            try self.assign(); // pop
        }

        pub fn unpackUint(self: *UnpackerImpl, v: u64) !void {
            if ((zlua.lang != .luau and zlua.lang != .lua52) or v > std.math.maxInt(zlua.Unsigned)) {
                std.debug.assert(v < 2e14);
                self.lua.pushNumber(@floatFromInt(v));
            } else {
                self.lua.pushUnsigned(@intCast(v)); // [+p]
            }
            try self.assign(); // pop
        }

        pub fn unpackFloat(self: *UnpackerImpl, v: f64) !void {
            self.lua.pushNumber(v); // [+p]
            try self.assign(); // pop
        }

        pub fn unpackStr(self: *UnpackerImpl, str: []const u8) !void {
            _ = self.lua.pushString(str); // [+p]
            try self.assign(); // pop
        }

        pub fn mapBegin(self: *UnpackerImpl, l: usize) !void {
            // Preallocate the table (num_rec is a hint to VM)
            self.lua.createTable(0, @intCast(l)); // [+p]
            const top = self.lua.getTop();
            try self.tables.append(top);
        }

        pub fn mapField(self: *UnpackerImpl, key: []const u8) !void {
            _ = self.lua.pushString(key); // [+p]
        }

        pub fn mapEnd(self: *UnpackerImpl) !void {
            const table = self.tables.pop() orelse unreachable;
            std.debug.assert(self.lua.getTop() == table);
            try self.assign();
        }

        pub fn arrayBegin(self: *UnpackerImpl, s: usize) !void {
            self.lua.createTable(@intCast(s), 0);
            const top = self.lua.getTop();
            try self.tables.append(top);
            try self.arrayIdx.append(1); // lua array index starts at 1
        }

        pub fn arrayEnd(self: *UnpackerImpl) !void {
            _ = self.arrayIdx.pop();
            const table = self.tables.pop() orelse unreachable;
            std.debug.assert(self.lua.getTop() == table);
            try self.assign();
        }

        fn assign(self: *UnpackerImpl) !void {
            if (self.tables.items.len == 0) {
                // Just a value: do nothing
                return;
            }
            const table_idx = self.tables.items[self.tables.items.len - 1];
            const top = self.lua.getTop();
            if (top - table_idx == 2) {
                // Table, key, value: set table[key] = value
                self.lua.setTable(table_idx); // pops 2
            } else if (top - table_idx == 1) {
                // Table, value: set table[i] = value
                const idx = self.arrayIdx.getLast();
                self.lua.rawSetIndex(table_idx, idx); // pops 1
                self.arrayIdx.items[self.arrayIdx.items.len - 1] = idx + 1;
            } else {
                // Just a value: do nothing
                std.debug.assert(top == table_idx);
            }
        }
    };

    const Unpacker = msgpack.Unpacker(UnpackerImpl, .{
        .nil = UnpackerImpl.unpackNil,
        .bool = UnpackerImpl.unpackBool,
        .int = UnpackerImpl.unpackInt,
        .uint = UnpackerImpl.unpackUint,
        .float = UnpackerImpl.unpackFloat,
        .str = UnpackerImpl.unpackStr,
        .mapBegin = UnpackerImpl.mapBegin,
        .mapField = UnpackerImpl.mapField,
        .mapEnd = UnpackerImpl.mapEnd,
        .arrayBegin = UnpackerImpl.arrayBegin,
        .arrayEnd = UnpackerImpl.arrayEnd,
    }, BufferedReader);

    msgpack_file: std.fs.File,
    unpacker: Unpacker,

    const f_iter = "iter";
    const meta_table = "MsgpackUnpackerMT";
    const g_new_ctx = "msgpack_unpacker";

    pub fn luauDetor(data: *anyopaque) void {
        const ctx: *MsgpackUnpacker = @ptrFromInt(@intFromPtr(data));
        ctx.msgpack_file.close();
        ctx.unpacker.ctx.deinit();
    }

    pub fn luaDetor(lua: *Lua) !c_int {
        const ctx = try lua.toUserdata(MsgpackUnpacker, 1);
        ctx.msgpack_file.close();
        ctx.unpacker.ctx.deinit();
        return 0;
    }

    fn newCtx(lua: *Lua) !i32 {
        const path = lua.toString(1) catch |err| return printLuaErr(lua, err);

        const ctx = switch (zlua.lang) {
            // Other runtimes use __gc metamethod to clean up userdata
            .luau => lua.newUserdataDtor(MsgpackUnpacker, zlua.wrap(MsgpackUnpacker.luauDetor)),
            .lua54 => lua.newUserdata(MsgpackUnpacker, 0),
            else => lua.newUserdata(MsgpackUnpacker),
        };

        ctx.msgpack_file = std.fs.cwd().openFile(path, .{ .mode = .read_only }) catch |err| {
            logger.warn("Failed to open file {s}: {}", .{ path, err });
            return zlua.Error.LuaFile;
        };
        ctx.unpacker = Unpacker.init(.{ .unbuffered_reader = ctx.msgpack_file.reader() }, UnpackerImpl.init(lua), lua.allocator());

        _ = lua.getMetatableRegistry(MsgpackUnpacker.meta_table); // [+p]
        lua.setMetatable(-2); // pop 1

        return 1;
    }

    fn iter(lua: *Lua) i32 {
        _ = lua.checkUserdata(Self, 1, Self.meta_table);
        lua.pushValue(1); // [+p]
        lua.pushClosure(zlua.wrap(Self.next), 1); // pop 1 & push fn
        return 1;
    }

    fn next(lua: *Lua) !i32 {
        const ctx = try lua.toUserdata(Self, Lua.upvalueIndex(1));
        _ = ctx.unpacker.next(1) catch |err| {
            if (err == error.EndOfStream) {
                return 0;
            }

            std.debug.dumpCurrentStackTrace(null);
            logger.err("Msgpack unpacker failed: {}", .{err});
            return if (err == error.OutOfMemory) zlua.Error.OutOfMemory else zlua.Error.LuaRuntime;
        };
        return 1;
    }
};

pub fn registerRt(lua: *Lua) !void {
    try lua.newMetatable(ScanCtx.meta_table); // [+p]
    lua.newTable(); // [+p]
    lua.pushFunction(zlua.wrap(scanDir)); // [+p]
    lua.setField(-2, ScanCtx.f_iter); // pop 1
    lua.setField(-2, "__index"); // pop 1
    if (zlua.lang != .luau) {
        lua.pushFunction(zlua.wrap(ScanCtx.luaDetor)); // [+p]
        lua.setField(-2, "__gc"); // pop 1
    }
    lua.pop(1); // pop meta_table

    lua.pushFunction(zlua.wrap(newScanCtx)); // [+p]
    lua.setGlobal(ScanCtx.g_new_ctx); // pop 1

    try lua.newMetatable(MsgpackUnpacker.meta_table); // [+p]
    lua.newTable(); // [+p]
    lua.pushFunction(zlua.wrap(MsgpackUnpacker.iter)); // [+p]
    lua.setField(-2, MsgpackUnpacker.f_iter); // pop 1
    lua.setField(-2, "__index"); // pop 1
    if (zlua.lang != .luau) {
        lua.pushFunction(zlua.wrap(MsgpackUnpacker.luaDetor)); // [+p]
        lua.setField(-2, "__gc"); // pop 1
    }
    lua.pop(1); // pop meta_table

    lua.pushFunction(zlua.wrap(MsgpackUnpacker.newCtx)); // [+p]
    lua.setGlobal(MsgpackUnpacker.g_new_ctx); // pop 1
}
