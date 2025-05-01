const std = @import("std");
const zlua = @import("zlua");
const Lua = @import("zlua").Lua;
const lua_rt = @import("lua_rt.zig");
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

const c_u8ptr = [*c]const u8;

pub const LoadedRow = extern struct {
    keys: [*c]c_u8ptr = null,
    data: [*c]c_u8ptr = null,
    sizes: [*c]c_uint = null,
    num_keys: c_uint = 0,
};

const Row = struct {
    const Entry = struct {
        key: [:0]const u8,
        data: []u8,
    };

    arena: std.heap.ArenaAllocator,
    ext_row: LoadedRow = .{},
    entries: std.ArrayListUnmanaged(Entry),
    num_fullfilled: usize = 0,

    const List = std.DoublyLinkedList(Row);

    pub fn initAlloc(base_alloc: std.mem.Allocator) !*Row.List.Node {
        var r = try base_alloc.create(Row.List.Node);
        errdefer base_alloc.destroy(r);
        r.data.arena = std.heap.ArenaAllocator.init(base_alloc);
        r.data.entries = try std.ArrayListUnmanaged(Entry).initCapacity(r.data.arena.allocator(), 8);
        r.data.num_fullfilled = 0;
        return r;
    }

    pub fn deinit(self: *Row) void {
        self.arena.deinit();
    }

    pub fn reset(self: *Row) !void {
        self.ext_row = .{};
        _ = self.arena.reset(.retain_capacity);
        self.entries = try std.ArrayListUnmanaged(Entry).initCapacity(self.arena.allocator(), 8);
        self.num_fullfilled = 0;
    }
};

pub const LuaDataLoader = struct {
    const Self = @This();

    const UserLoaderFn = struct {
        init_ctx: i32 = 0,
        row_generator: i32 = 0,
    };

    const YieldedFrom = union(enum) {
        open_file: struct {
            // args
            file: []const u8,
            // state
            sent_rid: u64 = 0,
        },
        close_file: struct {
            // args
            file_handle: u32,
        },
        add_entry: struct {
            // args
            key: [:0]const u8,
            offset: u64,
            size: u32,
            file_handle: u32,
            // state
            entry: ?*Row.Entry = null,
        },
        generic: struct {},
    };

    alloc: std.mem.Allocator,
    loader: LoaderCtx = undefined,
    lua: *Lua = undefined,

    u_loader_fn: UserLoaderFn = .{},
    u_ctx: i32 = 0,
    u_ctx_funcs_table: i32 = 0,
    u_resume_nargs: i32 = 0,
    u_yielded_from: ?YieldedFrom = null,
    u_completed: bool = false,

    queue_size_rows: usize = 4,
    in_progress_row: ?*Row.List.Node = null,
    queue: Row.List = .{},
    free_list: Row.List = .{},
    num_floating_rows: usize = 0,

    load_rid_to_row: std.AutoArrayHashMapUnmanaged(u64, *Row),

    last_instant: std.time.Instant,
    mbps_smoothed: f64 = 0.0,
    rows_delivered: usize = 0,

    const max_floating_rows: usize = 16;

    fn gOpenFile(lua: *Lua) !i32 {
        const loader = try lua.toUserdata(Self, 1);
        // We yield after this function & the string ref should live long enough
        const file_path = try lua.toString(2);

        loader.u_yielded_from = .{
            .open_file = .{
                .file = file_path,
            },
        };

        return 0;
    }

    fn gCloseFile(lua: *Lua) !i32 {
        const loader = try lua.toUserdata(Self, 1);
        const handle: u32 = @intCast(try lua.toUnsigned(2));
        loader.u_yielded_from = .{
            .close_file = .{
                .file_handle = handle,
            },
        };
        return 0;
    }

    fn gAddEntry(lua: *Lua) !i32 {
        const loader = try lua.toUserdata(Self, 1);
        const handle: u32 = @intCast(try lua.toUnsigned(2));
        // We yield after this function & the string ref should live long enough
        const key = try lua.toString(3);
        const offset: u64 = @intCast(try lua.toUnsigned(4));
        const size: u32 = @intCast(try lua.toUnsigned(5));

        loader.u_yielded_from = .{
            .add_entry = .{
                .file_handle = handle,
                .key = key,
                .offset = offset,
                .size = size,
            },
        };

        return 0;
    }

    fn gFinishRow(lua: *Lua) !i32 {
        const loader = try lua.toUserdata(Self, 1);
        try loader.newInprogressRow();
        loader.u_yielded_from = .{ .generic = .{} };
        return 0;
    }

    inline fn wrapCoyield(lua: *Lua, global_name: [:0]const u8, comptime cfn: fn (lua: *Lua) anyerror!i32) !void {
        var buf: [1024]u8 = undefined;

        lua.pushFunction(zlua.wrap(cfn)); // [+p]
        lua.setGlobal(global_name); // pop fn

        const fmt =
            \\return function(self, ...)
            \\  {s}(self.c_loader, ...)
            \\  return coroutine.yield()
            \\end
        ;

        const wrapped = try std.fmt.bufPrintZ(&buf, fmt, .{global_name});

        lua.loadString(wrapped) catch |err| return lua_rt.printLuaErr(lua, err);
        errdefer lua.pop(1); // pop the function
        lua.protectedCall(.{ .results = 1 }) catch |err| return lua_rt.printLuaErr(lua, err);
    }

    fn newInprogressRow(self: *Self) !void {
        if (self.in_progress_row) |r| {
            self.queue.append(r);
        }

        self.in_progress_row = self.free_list.popFirst();

        if (self.in_progress_row) |r| {
            try r.data.reset();
        } else {
            self.in_progress_row = try Row.initAlloc(self.alloc);
        }
    }

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
        return lua_rt.printLuaErr(self.lua, err);
    }

    // Tries to resume the generator coroutine
    // If the generator is completed, returns .ok
    // If the generator has pending unresolved yield (u_yielded_from != null), returns .yield
    // Otherwise we resume the generator and return the status
    fn resumeGenerator(self: *Self) !zlua.ResumeStatus {
        if (self.u_completed) {
            return .ok;
        }
        if (self.u_yielded_from != null) {
            return .yield;
        }

        const status = self.lua.resumeThread(null, self.u_resume_nargs) catch |err| return self.printLuaErr(err);
        self.u_resume_nargs = 0;

        switch (status) {
            .ok => {
                logger.info("Generator completed", .{});
                self.u_completed = true;
                return .ok;
            },
            .yield => {
                return .yield;
            },
        }
    }

    pub fn nextRow(self: *Self) !?*LoadedRow {
        while (true) {
            const n = self.queue.len;
            // Throttle & wait for IO if we have enough in-flight rows
            if (n < self.queue_size_rows) {
                if (try self.resumeGenerator() == .ok and n == 0) {
                    // Generator completed
                    return null;
                }
            }

            // Handle the reason we yielded
            if (self.u_yielded_from != null) {
                switch (self.u_yielded_from.?) {
                    .open_file => |*f| {
                        if (f.sent_rid == 0) {
                            // Haven't sent the req yet
                            if (self.loader.trySend(.{ .open_file = .{ .file_path = f.file } })) |rid| {
                                f.sent_rid = rid;
                            }
                        }
                        // We resume when we got the file handle back,
                        // Don't clear u_yielded_from
                    },
                    .close_file => |*f| {
                        if (self.loader.trySend(.{ .close_file = @bitCast(f.file_handle) })) |_| {
                            self.u_yielded_from = null;
                        }
                    },
                    .add_entry => |*e| {
                        const row = &self.in_progress_row.?.data;
                        if (e.entry == null) {
                            const row_alloc = row.arena.allocator();
                            const key = try row_alloc.dupeZ(u8, e.key);
                            e.key = key;
                            const row_data = try row_alloc.alignedAlloc(u8, 32, e.size);
                            try row.entries.append(row_alloc, .{
                                .key = key,
                                .data = row_data,
                            });
                            e.entry = &row.entries.items[row.entries.items.len - 1];
                        }
                        if (self.loader.trySend(.{
                            .read_block = .{
                                .file = @bitCast(e.file_handle),
                                .base = e.offset,
                                .result_buffer = e.entry.?.data, //
                            },
                        })) |rid| {
                            self.u_yielded_from = null;
                            try self.load_rid_to_row.put(self.alloc, rid, row);
                        }
                    },
                    .generic => self.u_yielded_from = null,
                }
            }

            // Poll the loader responses
            while (self.loader.tryRecv()) |resp| {
                // FIXME: make some of these errors recoverable
                const payload = try resp.payload;

                switch (payload) {
                    .open_file => |f| {
                        std.debug.assert((self.u_yielded_from orelse @panic("Unresolved open_file req")) == .open_file);
                        self.lua.pushUnsigned(@intCast(@as(u32, @bitCast(f))));
                        self.u_resume_nargs = 1;
                        self.u_yielded_from = null;
                    },
                    .read_block => {
                        const rid = resp.request_id;
                        const kv = self.load_rid_to_row.fetchSwapRemove(rid) orelse @panic("read_block rid not found in map");
                        const row = kv.value;
                        row.num_fullfilled += 1;
                    },
                }
            }

            if (self.queue.first) |f_node| {
                const first = &f_node.data;

                logger.debug("Q len: {}, first: fullfilled = {}, entries = {}", .{ self.queue.len, first.num_fullfilled, first.entries.items.len });

                if (first.num_fullfilled == first.entries.items.len) {
                    const node = self.queue.popFirst() orelse unreachable;
                    self.num_floating_rows += 1;
                    if (self.num_floating_rows > Self.max_floating_rows) {
                        std.debug.panic("Too many floating (owned by client) rows > max: {}", .{Self.max_floating_rows});
                    }
                    const row = &node.data;
                    const alloc = row.arena.allocator();
                    const entries = row.entries.items;
                    var bytes: u64 = 0;
                    row.ext_row = .{
                        .keys = @ptrCast(try alloc.alloc(c_u8ptr, entries.len)),
                        .data = @ptrCast(try alloc.alloc(c_u8ptr, entries.len)),
                        .sizes = @ptrCast(try alloc.alloc(u64, entries.len)),
                        .num_keys = @intCast(entries.len),
                    };
                    for (entries, 0..) |e, i| {
                        row.ext_row.keys[i] = @ptrCast(e.key);
                        row.ext_row.data[i] = @ptrCast(e.data);
                        row.ext_row.sizes[i] = @intCast(e.data.len);
                        bytes += e.data.len;
                    }

                    const now = try std.time.Instant.now();
                    const delta = now.since(self.last_instant);
                    self.last_instant = now;
                    const mbps = @as(f64, @floatFromInt(bytes)) * 1e-6 / (@as(f64, @floatFromInt(delta)) * 1e-9);
                    self.mbps_smoothed = mbps * 0.9 + self.mbps_smoothed * 0.1;
                    if (self.rows_delivered % 100 == 99) {
                        logger.info("{d:.1} MBytes/s (Instant: {d:.1})", .{ self.mbps_smoothed, mbps });
                    }
                    self.rows_delivered += 1;

                    logger.debug("Returning row @ {}", .{&row.ext_row});
                    return &row.ext_row;
                } else if (first.num_fullfilled > first.entries.items.len) {
                    @panic("Row has more fullfilled entries than total entries");
                }
            }

            std.atomic.spinLoopHint();
            std.Thread.yield() catch {};
        }
    }

    pub fn reclaimRow(self: *Self, c_row: *LoadedRow) void {
        const row: *Row = @fieldParentPtr("ext_row", c_row);
        const node: *Row.List.Node = @fieldParentPtr("data", row);
        self.free_list.append(node);
        self.num_floating_rows -= 1;
    }

    fn initLua(self: *Self, spec: LuaLoaderSpec) !void {
        self.lua.openLibs();
        lua_rt.registerRt(self.lua) catch |err| return self.printLuaErr(err);

        self.lua.createTable(0, 2); // [+p]
        self.lua.pushLightUserdata(self); // [+p]
        self.lua.setField(-2, "c_loader"); // pop the self
        try Self.wrapCoyield(self.lua, "loader_open_file", Self.gOpenFile); // [+p]
        self.lua.setField(-2, "open_file"); // pop the function
        try Self.wrapCoyield(self.lua, "loader_close_file", Self.gCloseFile); // [+p]
        self.lua.setField(-2, "close_file"); // pop the function
        try Self.wrapCoyield(self.lua, "loader_add_entry", Self.gAddEntry); // [+p]
        self.lua.setField(-2, "add_entry"); // pop the function
        try Self.wrapCoyield(self.lua, "loader_finish_row", Self.gFinishRow); // [+p]
        self.lua.setField(-2, "finish_row"); // pop the function
        self.lua.setGlobal("g_loader"); // pop 1 & move to global

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

        // Setup the generator as a coroutine
        _ = self.lua.rawGetIndex(zlua.registry_index, self.u_loader_fn.row_generator); // [+p]
        _ = self.lua.rawGetIndex(zlua.registry_index, self.u_ctx); // [+p]
        self.u_resume_nargs = 1;
        // at this point `resume` can start the generator
    }

    pub fn init(spec: LuaLoaderSpec, alloc: std.mem.Allocator) !*Self {
        var self = try alloc.create(Self);
        self.* = .{ .alloc = alloc, .load_rid_to_row = try std.AutoArrayHashMapUnmanaged(u64, *Row).init(alloc, &.{}, &.{}), .last_instant = try std.time.Instant.now() };
        errdefer alloc.destroy(self);

        try self.newInprogressRow();

        self.loader = try LoaderCtx.init(alloc);
        errdefer self.loader.deinit();
        try self.loader.start();

        // This init the Lua VM,
        // runs init_ctx, and setup generator coroutine
        self.lua = try Lua.init(alloc);
        errdefer self.lua.deinit();
        try self.initLua(spec);

        return self;
    }

    pub fn deinit(self: *Self) void {
        self.loader.deinit();
        self.lua.deinit();

        while (self.queue.popFirst()) |r| {
            r.data.deinit();
            self.alloc.destroy(r);
        }
        while (self.free_list.popFirst()) |r| {
            r.data.deinit();
            self.alloc.destroy(r);
        }
        if (self.in_progress_row) |r| {
            r.data.deinit();
            self.alloc.destroy(r);
        }
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
    c.alloc.destroy(c.loader);
    const alloc = c.alloc_ctx;
    switch (alloc) {
        .rel => {},
        .debug => {
            _ = c.alloc_ctx.debug.deinit();
        },
    }
    std.heap.c_allocator.destroy(c);
}

export fn ultarNextRow(c: *LuaLoaderCCtx) ?*LoadedRow {
    const row = c.loader.nextRow() catch |err| {
        logger.err("Error getting next row: {}", .{err});
        return @ptrFromInt(0);
    };
    if (row == null) {
        return @ptrFromInt(0);
    }
    return row;
}

export fn ultarReclaimRow(c: *LuaLoaderCCtx, c_row: *LoadedRow) void {
    c.loader.reclaimRow(c_row);
}
