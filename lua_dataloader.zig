const std = @import("std");
const zlua = @import("zlua");
const Lua = @import("zlua").Lua;

const dataloader = @import("dataloader.zig");
const LoaderCtx = dataloader.LoaderCtx;

const logger = std.log.scoped(.lua_dataloader);

pub const LuaLoaderSpec = extern struct {
    src: [*c]const u8,
    shard_list: [*c]const [*c]const u8,
    num_shards: c_uint,
    rank: c_uint,
    world_size: c_uint,
    debug: bool,
};

pub const LuaDataLoader = struct {
    const Self = @This();

    const UserLoaderFn = struct {
        init_ctx: i32 = 0,
        row_generator: i32 = 0,
    };

    loader: LoaderCtx,
    lua: *Lua,

    u_loader_fn: UserLoaderFn = .{},
    u_ctx: i32 = 0,

    fn getFieldAsFuncRef(self: *Self, table: i32, field_name: [:0]const u8) !i32 {
        _ = self.lua.getField(table, field_name);
        if (self.lua.isFunction(-1)) {
            return try self.lua.ref(-1);
        } else {
            const type_name = self.lua.typeName(self.lua.typeOf(-1));
            self.lua.pop(1);
            logger.err("Expected {s} to be a function, got {s}", .{ field_name, type_name });
            return zlua.Error.LuaError;
        }
    }

    fn printLuaErr(self: *Self, err: zlua.Error) zlua.Error {
        switch (err) {
            error.LuaError, error.LuaRuntime, error.LuaSyntax => {
                const err_msg = self.lua.toString(-1) catch "unknown error";
                logger.err("{}Error:\n{s}", .{ err, err_msg });
            },
            else => {
                logger.err("Lua op returned {}", .{err});
            },
        }
        std.debug.dumpCurrentStackTrace(null);
        return err;
    }

    fn initLua(self: *Self, spec: LuaLoaderSpec) !void {
        self.lua.openLibs();

        // Compile src to bytecode & load into VM
        const alloc = self.lua.allocator();
        const src: [:0]const u8 = std.mem.span(spec.src);
        const bc = zlua.compile(alloc, src, .{}) catch |err| {
            logger.err("Error compiling Lua source: {}", .{err});
            return err;
        };
        defer alloc.free(bc);

        self.lua.loadBytecode("loader_spec_src", bc) catch |err| return self.printLuaErr(err);

        // Call the bytecode that generates a context & its functions
        self.lua.protectedCall(.{ .results = 1 }) catch |err| return self.printLuaErr(err);

        // Check the result type is what we expect
        const table = self.lua.getTop();
        if (!self.lua.isTable(table)) {
            const type_name = self.lua.typeName(self.lua.typeOf(table));
            logger.err("Expected spec result to be a table, got {s}", .{type_name});
            return zlua.Error.LuaError;
        }

        self.u_loader_fn.init_ctx = try self.getFieldAsFuncRef(table, "init_ctx");
        self.u_loader_fn.row_generator = try self.getFieldAsFuncRef(table, "row_generator");

        self.lua.pop(1); // Get rid of the table

        // Initialize the user provided context
        _ = self.lua.rawGetIndex(zlua.registry_index, self.u_loader_fn.init_ctx);
        self.lua.pushUnsigned(@intCast(spec.rank));
        self.lua.pushUnsigned(@intCast(spec.world_size));
        self.lua.protectedCall(.{ .args = 2, .results = 1 }) catch |err| return self.printLuaErr(err);
        self.u_ctx = self.lua.ref(-1) catch |err| return self.printLuaErr(err); // pop & store ref of ret value (user context)
    }

    pub fn init(spec: LuaLoaderSpec, alloc: std.mem.Allocator) !*Self {
        var self = try alloc.create(Self);
        errdefer alloc.destroy(self);

        self.lua = try Lua.init(alloc);
        errdefer self.lua.deinit();
        try self.initLua(spec);

        self.loader = try LoaderCtx.init(alloc);
        errdefer self.loader.deinit();
        try self.loader.start();

        return self;
    }

    pub fn deinit(self: *Self) void {
        const alloc = self.lua.allocator();
        self.loader.join();
        self.loader.deinit();
        self.lua.deinit();
        alloc.destroy(self);
    }
};

const LuaLoaderCCtx = struct {
    alloc_ctx: union(enum) {
        rel: struct {},
        debug: std.heap.DebugAllocator(.{}),
    },
    alloc: std.mem.Allocator,
    loader: *LuaDataLoader,
};

fn createLuaLoader(spec: LuaLoaderSpec) !*LuaLoaderCCtx {
    logger.info("Creating lua loader with spec: {}", .{spec});
    const c = try std.heap.c_allocator.create(LuaLoaderCCtx);
    errdefer std.heap.c_allocator.destroy(c);

    if (spec.debug) {
        c.alloc_ctx = .{ .debug = std.heap.DebugAllocator(.{}).init };
        errdefer _ = c.alloc_ctx.debug.deinit();
        c.alloc = c.alloc_ctx.debug.allocator();
        c.loader = try LuaDataLoader.init(spec, c.alloc);
        errdefer c.loader.deinit();
        return c;
    } else {
        c.alloc_ctx = .{ .rel = .{} };
        c.alloc = std.heap.smp_allocator;
        c.loader = try LuaDataLoader.init(spec, c.alloc);
        errdefer c.loader.deinit();
        return c;
    }
}

export fn ultarCreateLuaLoader(spec: LuaLoaderSpec) ?*LuaLoaderCCtx {
    return createLuaLoader(spec) catch {
        return @ptrFromInt(0);
    };
}

export fn ultarDestroyLuaLoader(c: *LuaLoaderCCtx) void {
    c.loader.deinit();
    const alloc = c.alloc_ctx;
    switch (alloc) {
        .rel => {},
        .debug => {
            _ = c.alloc_ctx.debug.deinit();
        },
    }
    std.heap.c_allocator.destroy(c);
}
