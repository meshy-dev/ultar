const std = @import("std");
const zlua = @import("zlua");
const Lua = @import("zlua").Lua;

const logger = std.log.scoped(.lua_rt);

const ScanCtx = struct {
    dir: std.fs.Dir,
    iter: std.fs.Dir.Iterator,

    const g_new_ctx = "scan_dir";
    const f_iter = "iter";
    const meta_table = "ScanCtxMT";

    pub fn luaDetor(data: *anyopaque) void {
        const ctx: *ScanCtx = @ptrFromInt(@intFromPtr(data));
        ctx.dir.close();
    }
};

pub fn printLuaErr(lua: *Lua, err: zlua.Error) zlua.Error {
    std.debug.dumpCurrentStackTrace(null);
    switch (err) {
        error.LuaError, error.LuaRuntime, error.LuaSyntax => {
            const err_msg = lua.toString(-1) catch "unknown error";
            logger.err("{}Error:\n{s}", .{ err, err_msg });
        },
        else => {
            logger.err("Lua op returned {}", .{err});
        },
    }
    return err;
}

fn newScanCtx(lua: *Lua) !i32 {
    const path = lua.toString(1) catch |err| return printLuaErr(lua, err);

    const ctx = lua.newUserdataDtor(ScanCtx, zlua.wrap(ScanCtx.luaDetor));
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
    lua.pushValue(1);
    lua.pushClosure(zlua.wrap(scanIterator), 1);
    return 1;
}

pub fn registerRt(lua: *Lua) !void {
    try lua.newMetatable(ScanCtx.meta_table); // [+p]
    lua.newTable(); // [+p]
    lua.pushFunction(zlua.wrap(scanDir)); // [+p]
    lua.setField(-2, ScanCtx.f_iter); // pop 1
    lua.setField(-2, "__index"); // pop 1
    lua.pop(1); // pop meta_table

    lua.pushFunction(zlua.wrap(newScanCtx)); // [+p]
    lua.setGlobal(ScanCtx.g_new_ctx); // pop 1
}
