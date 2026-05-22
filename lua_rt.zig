const std = @import("std");
const zlua = @import("zlua");
const Lua = @import("zlua").Lua;
const msgpack = @import("msgpack.zig");

const logger = std.log.scoped(.lua_rt);

const max_exact_lua_handle = std.math.maxInt(u48);
const can_use_lua_unsigned64 =
    (zlua.lang == .luau or zlua.lang == .lua52) and std.math.maxInt(zlua.Unsigned) >= max_exact_lua_handle;

/// Lua-side runtime for ultar. Bundles the `Lua` state with the `Io` used by
/// callbacks and the modules registered onto the state. Stable pointer:
/// callers heap-allocate (or place at a stable address) so userdata that
/// outlives a single call can hold `*LuaRt` directly.
pub const LuaRt = struct {
    lua: *Lua,
    io: std.Io,

    const rt_registry_key: [:0]const u8 = "ultar.lua_rt.rt_ptr";

    /// In-place init. Stashes `self` in `lua`'s registry and registers the
    /// `ultar.*` modules and userdata metatables. `self` must outlive `lua`.
    pub fn init(self: *LuaRt, lua: *Lua, io: std.Io) !void {
        self.lua = lua;
        self.io = io;
        lua.pushLightUserdata(@ptrCast(self)); // [+p]
        lua.setField(zlua.registry_index, rt_registry_key); // pop 1
        try self.registerModules();
    }

    /// Fetch the runtime previously installed on `lua`.
    pub fn fromLua(lua: *Lua) *LuaRt {
        _ = lua.getField(zlua.registry_index, rt_registry_key); // [+p]
        const ptr = lua.toPointer(-1) orelse @panic("lua_rt: runtime not installed");
        lua.pop(1); // pop 1
        return @ptrCast(@alignCast(@constCast(ptr)));
    }

    fn registerModules(self: *LuaRt) !void {
        const lua = self.lua;
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

        try registerPreload(lua, "ultar.utix", zlua.wrap(utixModuleLoader));
        try registerPreload(lua, "ultar.scandir", zlua.wrap(scandirModuleLoader));
        try registerPreload(lua, "ultar.debug", zlua.wrap(debugModuleLoader));
    }
};

fn dumpStackCfn(lua: *Lua) i32 {
    luaDumpStack(lua);
    return 0;
}

/// Lua loader for `ultar.debug`; returns `{ dump_stack = fn() }`. Calling
/// `dump_stack` from inside an active Lua frame yields a non-empty call-stack
/// section (the dump runs before any error unwind).
fn debugModuleLoader(lua: *Lua) i32 {
    lua.createTable(0, 1); // [+p] module table
    lua.pushFunction(zlua.wrap(dumpStackCfn)); // [+p]
    lua.setField(-2, "dump_stack"); // pop dump_stack
    return 1;
}

const ScanCtx = struct {
    rt: *LuaRt,
    dir: std.Io.Dir,
    iter: std.Io.Dir.Iterator,

    const g_new_ctx = "scan_dir";
    const f_iter = "iter";
    const meta_table = "ScanCtxMT";

    pub fn luauDetor(data: *anyopaque) void {
        const ctx: *ScanCtx = @ptrFromInt(@intFromPtr(data));
        ctx.dir.close(ctx.rt.io);
    }

    pub fn luaDetor(lua: *Lua) !c_int {
        const ctx = try lua.toUserdata(ScanCtx, 1);
        ctx.dir.close(ctx.rt.io);
        return 0;
    }
};

/// Logs the active call stack (frames currently executing) followed by every
/// value slot on the data stack, with absolute and relative indices, type,
/// and a best-effort summary. Stack-safe: any temporary pushes are popped
/// before returning.
pub fn luaDumpStack(lua: *Lua) void {
    dumpCallStack(lua);
    const top = lua.getTop();
    logger.info("Lua data stack (top={d}):", .{top});
    if (top <= 0) {
        logger.info("  <empty>", .{});
        return;
    }
    var buf: [512]u8 = undefined;
    var i: i32 = 1;
    while (i <= top) : (i += 1) {
        const rel = i - top - 1; // -1 = top, -2 = top-1, ...
        const desc = describeSlot(lua, i, &buf);
        logger.info("  [{d:>3} | {d:>3}] {s}", .{ i, rel, desc });
    }
}

/// Walks the activation records (lowest level = innermost current call) and
/// logs each. Distinct from the data stack: an entry here is a function
/// *currently being called*, not a function value sitting in a slot.
fn dumpCallStack(lua: *Lua) void {
    logger.info("Lua call stack (innermost first):", .{});
    var level: i32 = 0;
    var seen: i32 = 0;
    while (level < 64) : (level += 1) {
        var buf: [256]u8 = undefined;
        const line = callFrameDescribe(lua, level, &buf) orelse break;
        logger.info("  #{d} {s}", .{ level, line });
        seen += 1;
    }
    if (seen == 0) logger.info("  <no active Lua frames>", .{});
}

fn callFrameDescribe(lua: *Lua, level: i32, buf: []u8) ?[]const u8 {
    if (zlua.lang == .luau) {
        // Luau's getInfo takes the level directly and uses lowercase `s`.
        var info: zlua.DebugInfo = .{};
        lua.getInfo(level, .{ .s = true, .l = true, .n = true }, &info);
        return formatFrame(&info, buf);
    }
    var info = lua.getStack(level) catch return null;
    lua.getInfo(.{ .S = true, .l = true, .n = true }, &info);
    return formatFrame(&info, buf);
}

fn formatFrame(info: anytype, buf: []u8) []const u8 {
    const what_str = @tagName(info.what);
    const name: []const u8 = if (info.name) |n| n else "?";
    const line: i32 = info.current_line orelse -1;
    const short_src = std.mem.sliceTo(&info.short_src, 0);
    return std.fmt.bufPrint(buf, "{s} '{s}' at {s}:{d}", .{ what_str, name, short_src, line }) catch "<frame>";
}

fn describeSlot(lua: *Lua, idx: i32, buf: []u8) []const u8 {
    return switch (lua.typeOf(idx)) {
        .none => "none",
        .nil => "nil",
        .boolean => if (lua.toBoolean(idx)) "boolean true" else "boolean false",
        .light_userdata => describePointer(lua, idx, "lightuserdata", buf),
        .number => describeNumber(lua, idx, buf),
        .string => describeString(lua, idx, buf),
        .table => describeTable(lua, idx, buf),
        .function => describeFunction(lua, idx, buf),
        .userdata => describeUserdata(lua, idx, buf),
        .thread => "thread",
    };
}

fn describePointer(lua: *Lua, idx: i32, kind: []const u8, buf: []u8) []const u8 {
    const ptr = lua.toPointer(idx) orelse return std.fmt.bufPrint(buf, "{s} 0x0", .{kind}) catch kind;
    return std.fmt.bufPrint(buf, "{s} 0x{x}", .{ kind, @intFromPtr(ptr) }) catch kind;
}

fn describeNumber(lua: *Lua, idx: i32, buf: []u8) []const u8 {
    const v = lua.toNumber(idx) catch return "number <unreadable>";
    if (std.math.isFinite(v) and @floor(v) == v and v >= -1e15 and v <= 1e15) {
        const iv: i64 = @intFromFloat(v);
        return std.fmt.bufPrint(buf, "number {d}", .{iv}) catch "number";
    }
    return std.fmt.bufPrint(buf, "number {d}", .{v}) catch "number";
}

fn describeString(lua: *Lua, idx: i32, buf: []u8) []const u8 {
    const s = lua.toString(idx) catch return "string <unreadable>";
    const cap = 60;
    if (s.len <= cap) {
        return std.fmt.bufPrint(buf, "string({d}) \"{s}\"", .{ s.len, s }) catch "string";
    }
    return std.fmt.bufPrint(buf, "string({d}) \"{s}…\"", .{ s.len, s[0..cap] }) catch "string";
}

fn describeTable(lua: *Lua, idx: i32, buf: []u8) []const u8 {
    const array_len = lua.lenRaw(idx);
    var name_buf: [64]u8 = undefined;
    if (metatableName(lua, idx, &name_buf)) |name| {
        return std.fmt.bufPrint(buf, "table[len={d}] mt=<{s}>", .{ array_len, name }) catch "table";
    }
    return std.fmt.bufPrint(buf, "table[len={d}]", .{array_len}) catch "table";
}

fn describeFunction(lua: *Lua, idx: i32, buf: []u8) []const u8 {
    const ptr = lua.toPointer(idx);
    const addr = if (ptr) |p| @intFromPtr(p) else 0;
    const kind: []const u8 = if (lua.isCFunction(idx)) "C function" else "Lua function";

    // A function on the data stack is a *value* — it's not currently
    // executing. Use getInfo's `>` mode (Lua / luajit only) to surface where
    // it was defined so the dump distinguishes a Lua-function value
    // (`defined script.lua:12-30`) from an in-progress call (which shows up
    // in the call stack section above).
    if (zlua.lang != .luau) {
        lua.pushValue(idx); // [+p] function value; `>` consumes it.
        var info: zlua.DebugInfo = .{};
        lua.getInfo(.{ .@">" = true, .S = true }, &info);
        const short_src = std.mem.sliceTo(&info.short_src, 0);
        const first = info.first_line_defined orelse 0;
        const last = info.last_line_defined orelse 0;
        return std.fmt.bufPrint(buf, "value: {s} 0x{x} defined {s}:{d}-{d}", .{ kind, addr, short_src, first, last }) catch kind;
    }
    return std.fmt.bufPrint(buf, "value: {s} 0x{x}", .{ kind, addr }) catch kind;
}

fn describeUserdata(lua: *Lua, idx: i32, buf: []u8) []const u8 {
    const ptr = lua.toPointer(idx);
    const addr = if (ptr) |p| @intFromPtr(p) else 0;
    var name_buf: [64]u8 = undefined;
    if (metatableName(lua, idx, &name_buf)) |name| {
        return std.fmt.bufPrint(buf, "userdata <{s}> 0x{x}", .{ name, addr }) catch "userdata";
    }
    return std.fmt.bufPrint(buf, "userdata 0x{x}", .{addr}) catch "userdata";
}

/// Best-effort metatable `__name` lookup. Leaves the stack unchanged; copies
/// the name into `out` so the result is stable once the metatable is popped.
fn metatableName(lua: *Lua, idx: i32, out: []u8) ?[]const u8 {
    lua.getMetatable(idx) catch return null; // [+p] metatable
    defer lua.pop(1);
    _ = lua.getField(-1, "__name"); // [+p] name (or nil)
    defer lua.pop(1);
    if (!lua.isString(-1)) return null;
    const s = lua.toString(-1) catch return null;
    const n = @min(s.len, out.len);
    @memcpy(out[0..n], s[0..n]);
    return out[0..n];
}

pub fn toUnsigned(lua: *Lua, idx: i32) !u32 {
    if (zlua.lang == .luau or zlua.lang == .lua52) {
        return lua.toUnsigned(idx);
    } else {
        const f = try lua.toNumber(idx);
        return @intFromFloat(f);
    }
}

pub fn toUnsigned64(lua: *Lua, idx: i32) !u64 {
    if (can_use_lua_unsigned64) {
        return lua.toUnsigned(idx);
    } else {
        const f = try lua.toNumber(idx);
        std.debug.assert(f >= 0 and f <= max_exact_lua_handle);
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

pub fn pushUnsigned64(lua: *Lua, v: u64) void {
    if (can_use_lua_unsigned64) {
        lua.pushUnsigned(@intCast(v));
    } else {
        std.debug.assert(v <= max_exact_lua_handle);
        const f: f64 = @floatFromInt(v);
        lua.pushNumber(f);
    }
}

pub fn printLuaErr(lua: *Lua, err: anyerror) anyerror {
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
    std.debug.dumpCurrentStackTrace(.{});
    return err;
}

fn newScanCtx(lua: *Lua) !i32 {
    const path = lua.toString(1) catch |err| return printLuaErr(lua, err);
    const rt = LuaRt.fromLua(lua);

    const ctx = switch (zlua.lang) {
        // Non-luau runtimes attach cleanup via the __gc metamethod instead.
        .luau => lua.newUserdataDtor(ScanCtx, zlua.wrap(ScanCtx.luauDetor)),
        .lua54 => lua.newUserdata(ScanCtx, 0),
        else => lua.newUserdata(ScanCtx),
    };

    ctx.rt = rt;
    ctx.dir = std.Io.Dir.cwd().openDir(rt.io, path, .{ .iterate = true }) catch |err| {
        logger.warn("Failed to open directory {s}: {}", .{ path, err });
        return error.LuaFile;
    };
    ctx.iter = ctx.dir.iterate();

    _ = lua.getMetatableRegistry(ScanCtx.meta_table);
    lua.setMetatable(-2);

    return 1;
}

fn scanIterator(lua: *Lua) !i32 {
    const ctx = try lua.toUserdata(ScanCtx, Lua.upvalueIndex(1));
    const next_e = try ctx.iter.next(ctx.rt.io);
    if (next_e) |entry| {
        var buf: [std.Io.Dir.max_path_bytes]u8 = undefined;
        const len = try ctx.dir.realPathFile(ctx.rt.io, entry.name, &buf);
        _ = lua.pushString(buf[0..len]);
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

    pub const UnpackerImpl = struct {
        lua: *Lua,
        tables: std.ArrayList(i32),
        arrayIdx: std.ArrayList(i32),

        pub fn init(lua: *Lua) UnpackerImpl {
            return .{
                .lua = lua,
                .tables = std.ArrayList(i32).empty,
                .arrayIdx = std.ArrayList(i32).empty,
            };
        }

        pub fn deinit(self: *UnpackerImpl) void {
            const alloc = self.lua.allocator();
            self.tables.deinit(alloc);
            self.arrayIdx.deinit(alloc);
        }

        pub fn reset(self: *UnpackerImpl) void {
            const alloc = self.lua.allocator();
            self.tables.clearRetainingCapacity(alloc);
            self.arrayIdx.clearRetainingCapacity(alloc);
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
            self.lua.createTable(0, @intCast(l)); // [+p]
            const top = self.lua.getTop();
            try self.tables.append(self.lua.allocator(), top);
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
            const alloc = self.lua.allocator();
            try self.tables.append(alloc, top);
            try self.arrayIdx.append(alloc, 1); // Lua arrays are 1-indexed.
        }

        pub fn arrayEnd(self: *UnpackerImpl) !void {
            _ = self.arrayIdx.pop();
            const table = self.tables.pop() orelse unreachable;
            std.debug.assert(self.lua.getTop() == table);
            try self.assign();
        }

        fn assign(self: *UnpackerImpl) !void {
            if (self.tables.items.len == 0) return;
            const table_idx = self.tables.items[self.tables.items.len - 1];
            const top = self.lua.getTop();
            if (top - table_idx == 2) {
                // [table, key, value] -> table[key] = value
                self.lua.setTable(table_idx); // pops 2
            } else if (top - table_idx == 1) {
                // [table, value] -> table[i] = value
                const idx = self.arrayIdx.getLast();
                self.lua.setIndexRaw(table_idx, idx); // pops 1
                self.arrayIdx.items[self.arrayIdx.items.len - 1] = idx + 1;
            } else {
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
    });

    rt: *LuaRt,
    msgpack_file: std.Io.File,
    unpacker: Unpacker,
    read_buf: [65536]u8 = undefined,
    reader: std.Io.File.Reader,

    const f_iter = "iter";
    const meta_table = "MsgpackUnpackerMT";
    const g_new_ctx = "msgpack_unpacker";

    pub fn luauDetor(data: *anyopaque) void {
        const ctx: *MsgpackUnpacker = @ptrFromInt(@intFromPtr(data));
        ctx.msgpack_file.close(ctx.rt.io);
        ctx.unpacker.ctx.deinit();
    }

    pub fn luaDetor(lua: *Lua) !c_int {
        const ctx = try lua.toUserdata(MsgpackUnpacker, 1);
        ctx.msgpack_file.close(ctx.rt.io);
        ctx.unpacker.ctx.deinit();
        return 0;
    }

    fn newCtx(lua: *Lua) !i32 {
        const path = lua.toString(1) catch |err| return printLuaErr(lua, err);
        const rt = LuaRt.fromLua(lua);

        const ctx = switch (zlua.lang) {
            // Non-luau runtimes attach cleanup via the __gc metamethod instead.
            .luau => lua.newUserdataDtor(MsgpackUnpacker, zlua.wrap(MsgpackUnpacker.luauDetor)),
            .lua54 => lua.newUserdata(MsgpackUnpacker, 0),
            else => lua.newUserdata(MsgpackUnpacker),
        };

        ctx.rt = rt;
        ctx.msgpack_file = std.Io.Dir.cwd().openFile(rt.io, path, .{ .mode = .read_only }) catch |err| {
            logger.warn("Failed to open file {s}: {}", .{ path, err });
            return error.LuaFile;
        };
        ctx.reader = ctx.msgpack_file.readerStreaming(rt.io, &ctx.read_buf);
        ctx.unpacker = .{ .reader = &ctx.reader.interface, .ctx = UnpackerImpl.init(lua), .alloc = lua.allocator() };

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

            std.debug.dumpCurrentStackTrace(.{});
            logger.err("Msgpack unpacker failed: {}", .{err});
            return if (err == error.OutOfMemory) error.OutOfMemory else error.LuaRuntime;
        };
        return 1;
    }
};

/// Lua loader for `ultar.utix`; returns `{ open = fn(path) }`.
fn utixModuleLoader(lua: *Lua) i32 {
    lua.createTable(0, 1); // [+p] module table
    lua.pushFunction(zlua.wrap(MsgpackUnpacker.newCtx)); // [+p]
    lua.setField(-2, "open"); // pop, set module.open
    return 1;
}

/// Lua loader for `ultar.scandir`; returns `{ open = fn(path) }`.
fn scandirModuleLoader(lua: *Lua) i32 {
    lua.createTable(0, 1); // [+p] module table
    lua.pushFunction(zlua.wrap(newScanCtx)); // [+p]
    lua.setField(-2, "open"); // pop, set module.open
    return 1;
}

/// Install `loader_fn` at `package.preload[name]`.
fn registerPreload(lua: *Lua, name: [:0]const u8, loader_fn: zlua.CFn) !void {
    _ = lua.getGlobal("package"); // [+p]
    _ = lua.getField(-1, "preload"); // [+p]
    lua.pushFunction(loader_fn); // [+p]
    lua.setField(-2, name); // pop loader_fn
    lua.pop(2); // pop preload, package
}
