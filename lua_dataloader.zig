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
    // Key-value config passed from Python as parallel arrays
    // All values are strings; Lua script can convert as needed
    config_keys: [*c]const [*c]const u8 = null,
    config_values: [*c]const [*c]const u8 = null,
    config_count: c_uint = 0,
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

    node: std.DoublyLinkedList.Node = .{},
    arena: std.heap.ArenaAllocator,
    ext_row: LoadedRow = .{},
    entries: std.ArrayListUnmanaged(Entry),
    num_fullfilled: usize = 0,

    pub fn initAlloc(base_alloc: std.mem.Allocator) !*Row {
        var r = try base_alloc.create(Row);
        errdefer base_alloc.destroy(r);
        r.arena = std.heap.ArenaAllocator.init(base_alloc);
        r.entries = try std.ArrayListUnmanaged(Entry).initCapacity(r.arena.allocator(), 8);
        r.num_fullfilled = 0;
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
    in_progress_row: ?*Row = null,
    queue: std.DoublyLinkedList = .{},
    queue_len: usize = 0,
    free_list: std.DoublyLinkedList = .{},
    num_floating_rows: usize = 0,

    load_rid_to_row: std.AutoArrayHashMapUnmanaged(u64, *Row),

    last_instant: std.time.Instant,
    last_log_instant: std.time.Instant,
    mbps_smoothed: f64 = 0.0,
    mbps_period_max: f64 = 0.0,
    samples_count: u64 = 0,

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
        const handle: u32 = try lua_rt.toUnsigned(lua, 2);
        loader.u_yielded_from = .{
            .close_file = .{
                .file_handle = handle,
            },
        };
        return 0;
    }

    fn gAddEntry(lua: *Lua) !i32 {
        const loader = try lua.toUserdata(Self, 1);
        const handle: u32 = try lua_rt.toUnsigned(lua, 2);
        // We yield after this function & the string ref should live long enough
        const key = try lua.toString(3);
        const offset: u64 = @intFromFloat(try lua.toNumber(4));
        const size: u32 = try lua_rt.toUnsigned(lua, 5);

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
            \\    assert(
            \\        type(self) == "table" and type(self.c_loader) == "userdata",
            \\        "Invalid self, use `loader:method()` not `loader.method()`"
            \\    );
            \\    {s}(self.c_loader, ...)
            \\    return coroutine.yield()
            \\end
        ;

        const wrapped = try std.fmt.bufPrintZ(&buf, fmt, .{global_name});

        lua.loadString(wrapped) catch |err| return lua_rt.printLuaErr(lua, err);
        errdefer lua.pop(1); // pop the function
        lua.protectedCall(.{ .results = 1 }) catch |err| return lua_rt.printLuaErr(lua, err);
    }

    fn newInprogressRow(self: *Self) !void {
        if (self.in_progress_row) |r| {
            self.queue.append(&r.node);
            self.queue_len += 1;
        }

        const free_node_opt = self.free_list.popFirst();
        if (free_node_opt) |free_node| {
            var row: *Row = @fieldParentPtr("node", free_node);
            try row.reset();
            self.in_progress_row = row;
        } else {
            self.in_progress_row = try Row.initAlloc(self.alloc);
        }
    }

    fn luaPopAndRef(self: *Self) !i32 {
        // Pops the top value from the Lua stack and returns a reference to it.
        // This is used to store the result of a Lua function call in the registry.
        if (zlua.lang == .luau) {
            const ref = try self.lua.ref(-1);
            self.lua.pop(1); // pop the value
            return ref;
        } else {
            return try self.lua.ref(zlua.registry_index);
        }
    }

    // This function retrieves a function reference from the Lua table at `table` with the name `field_name`.
    // The function reference is stored in the registry and returned as an integer.
    fn getFieldAsFuncRef(self: *Self, table: i32, field_name: [:0]const u8) !i32 {
        _ = self.lua.getField(table, field_name);
        if (self.lua.isFunction(-1)) {
            return self.luaPopAndRef(); // Pop the function and return its reference
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

        var status: zlua.ResumeStatus = .ok;
        if (zlua.lang == .lua54) {
            var n_results: i32 = 0;
            status = self.lua.resumeThread(null, self.u_resume_nargs, &n_results) catch |err| return self.printLuaErr(err);
        } else if (zlua.lang == .luajit or zlua.lang == .lua51) {
            status = self.lua.resumeThread(self.u_resume_nargs) catch |err| return self.printLuaErr(err);
        } else {
            status = self.lua.resumeThread(null, self.u_resume_nargs) catch |err| return self.printLuaErr(err);
        }
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
        var wait_time_ns: u64 = 1_024; // Start with 1us
        const wait_time_cap: u64 = 1 << 24; // Cap at ~16ms
        while (true) {
            const n = self.queue_len;
            // Throttle & wait for IO if we have enough in-flight rows
            // Compute wait time heuristic
            if (n < self.queue_size_rows) {
                if (try self.resumeGenerator() == .ok and n == 0) {
                    // Generator completed
                    return null;
                }
                wait_time_ns = @max(wait_time_ns / 2, 1);
            } else {
                wait_time_ns = @min(wait_time_ns * 2, wait_time_cap);
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
                        const row = self.in_progress_row orelse @panic("No in-progress row while trying to .add_entry");
                        if (e.entry == null) {
                            const row_alloc = row.arena.allocator();
                            const key = try row_alloc.dupeZ(u8, e.key);
                            e.key = key;
                            const row_data = try row_alloc.alignedAlloc(u8, .fromByteUnits(32), e.size);
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
                                .result_buffer = (e.entry orelse @panic("Entry is null in .add_entry")).data, //
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
                        lua_rt.pushUnsigned(self.lua, @intCast(@as(u32, @bitCast(f))));
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
                const first: *Row = @fieldParentPtr("node", f_node);

                logger.debug("Q len: {}, first: fullfilled = {}, entries = {}", .{ self.queue_len, first.num_fullfilled, first.entries.items.len });

                if (first.num_fullfilled == first.entries.items.len) {
                    const node = self.queue.popFirst() orelse unreachable;
                    self.queue_len -= 1;
                    self.num_floating_rows += 1;
                    if (self.num_floating_rows > Self.max_floating_rows) {
                        std.debug.panic("Too many floating (owned by client) rows > max: {}", .{Self.max_floating_rows});
                    }
                    const row: *Row = @fieldParentPtr("node", node);
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

                    // Adaptive smoothing: alpha = 1/min(samples, 100)
                    self.samples_count += 1;
                    const smoothing_samples = @min(self.samples_count, 100);
                    const alpha = 1.0 / @as(f64, @floatFromInt(smoothing_samples));
                    self.mbps_smoothed = alpha * mbps + (1.0 - alpha) * self.mbps_smoothed;

                    // Track max throughput during this logging period
                    self.mbps_period_max = @max(self.mbps_period_max, mbps);

                    // Log throughput every 60 seconds
                    const since_last_log = now.since(self.last_log_instant);
                    if (since_last_log >= 60 * std.time.ns_per_s) {
                        logger.info("{d:.1} MBytes/s (Period max: {d:.1})", .{ self.mbps_smoothed, self.mbps_period_max });
                        self.last_log_instant = now;
                        self.mbps_period_max = 0.0; // Reset for next period
                    }

                    logger.debug("Returning row @ {}", .{&row.ext_row});
                    return &row.ext_row;
                } else if (first.num_fullfilled > first.entries.items.len) {
                    @panic("Row has more fullfilled entries than total entries");
                }
            }

            if (wait_time_ns < 10_000) {
                std.atomic.spinLoopHint();
            }
            std.Thread.sleep(wait_time_ns);
        }
    }

    pub fn reclaimRow(self: *Self, c_row: *LoadedRow) void {
        const row: *Row = @fieldParentPtr("ext_row", c_row);
        self.free_list.append(&row.node);
        self.num_floating_rows -= 1;
    }

    /// Module loader for ultar.loader - returns the loader interface table
    fn loaderModuleLoader(lua: *Lua) !i32 {
        // Get self from upvalue
        const self = try lua.toUserdata(Self, Lua.upvalueIndex(1));

        // Create the module table with loader methods
        lua.createTable(0, 5); // [+p] module table

        // Store c_loader reference for method calls
        lua.pushLightUserdata(self); // [+p]
        lua.setField(-2, "c_loader"); // pop

        // Add methods
        try Self.wrapCoyield(lua, "loader_open_file", Self.gOpenFile); // [+p]
        lua.setField(-2, "open_file"); // pop
        try Self.wrapCoyield(lua, "loader_close_file", Self.gCloseFile); // [+p]
        lua.setField(-2, "close_file"); // pop
        try Self.wrapCoyield(lua, "loader_add_entry", Self.gAddEntry); // [+p]
        lua.setField(-2, "add_entry"); // pop
        try Self.wrapCoyield(lua, "loader_finish_row", Self.gFinishRow); // [+p]
        lua.setField(-2, "finish_row"); // pop

        return 1; // return module table
    }

    /// Register the loader module in package.preload["ultar.loader"]
    fn registerLoaderModule(self: *Self) !void {
        _ = try self.lua.getGlobal("package"); // [+p]
        _ = self.lua.getField(-1, "preload"); // [+p]

        // Create closure with self as upvalue
        self.lua.pushLightUserdata(self); // [+p] upvalue
        self.lua.pushClosure(zlua.wrap(Self.loaderModuleLoader), 1); // pop upvalue, push closure

        self.lua.setField(-2, "ultar.loader"); // pop closure
        self.lua.pop(2); // pop preload, package
    }

    fn initLua(self: *Self, spec: LuaLoaderSpec) !void {
        self.lua.openLibs();
        lua_rt.registerRt(self.lua) catch |err| return self.printLuaErr(err);

        // Register ultar.loader module
        self.registerLoaderModule() catch |err| return self.printLuaErr(err);

        // Compile src to bytecode & load into VM
        if (zlua.lang == .luau) {
            const alloc = self.lua.allocator();
            const src: [:0]const u8 = std.mem.span(spec.src);
            const bc = zlua.compile(alloc, src, .{}) catch |err| {
                logger.err("Error compiling Lua source: {}", .{err});
                return err;
            };
            defer alloc.free(bc);

            self.lua.loadBytecode("loader_spec_src", bc) catch |err| return self.printLuaErr(err);
        } else {
            const src: [:0]const u8 = std.mem.span(spec.src);
            self.lua.loadString(src) catch |err| return self.printLuaErr(err);
        }

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
        // Call init_ctx(rank, world_size, config)
        _ = self.lua.rawGetIndex(zlua.registry_index, self.u_loader_fn.init_ctx);
        lua_rt.pushUnsigned(self.lua, @intCast(spec.rank));
        lua_rt.pushUnsigned(self.lua, @intCast(spec.world_size));

        // Push config table as 3rd argument
        self.lua.createTable(@intCast(spec.config_count), 0); // [+p]
        if (spec.config_keys != null and spec.config_values != null) {
            for (0..spec.config_count) |i| {
                const key: [*:0]const u8 = @ptrCast(spec.config_keys[i]);
                const value: [*:0]const u8 = @ptrCast(spec.config_values[i]);
                _ = self.lua.pushString(std.mem.span(value)); // [+p]
                self.lua.setField(-2, std.mem.span(key)); // pop value
            }
        }

        self.lua.protectedCall(.{ .args = 3, .results = 1 }) catch |err| return self.printLuaErr(err);
        self.u_ctx = self.luaPopAndRef() catch |err| return self.printLuaErr(err); // pop & store ref of ret value (user context)

        // Setup the generator as a coroutine
        _ = self.lua.rawGetIndex(zlua.registry_index, self.u_loader_fn.row_generator); // [+p]
        _ = self.lua.rawGetIndex(zlua.registry_index, self.u_ctx); // [+p]
        self.u_resume_nargs = 1;
        // at this point `resume` can start the generator
    }

    pub fn init(spec: LuaLoaderSpec, alloc: std.mem.Allocator) !*Self {
        var self = try alloc.create(Self);
        const now = try std.time.Instant.now();
        self.* = .{ .alloc = alloc, .load_rid_to_row = try std.AutoArrayHashMapUnmanaged(u64, *Row).init(alloc, &.{}, &.{}), .last_instant = now, .last_log_instant = now };
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
        self.load_rid_to_row.deinit(self.alloc);
        self.loader.deinit();
        self.lua.deinit();

        while (self.queue.popFirst()) |r_node| {
            var r: *Row = @fieldParentPtr("node", r_node);
            r.deinit();
            self.alloc.destroy(r);
        }
        self.queue_len = 0;
        while (self.free_list.popFirst()) |r_node| {
            var r: *Row = @fieldParentPtr("node", r_node);
            r.deinit();
            self.alloc.destroy(r);
        }
        if (self.in_progress_row) |r| {
            r.deinit();
            self.alloc.destroy(r);
        }
    }
};

pub const LuaLoaderCCtx = struct {
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

pub export fn ultarCreateLuaLoader(spec: LuaLoaderSpec) ?*LuaLoaderCCtx {
    return createLuaLoader(spec) catch {
        std.debug.dumpCurrentStackTrace(null);
        return @ptrFromInt(0);
    };
}

pub export fn ultarDestroyLuaLoader(c: *LuaLoaderCCtx) void {
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

pub export fn ultarNextRow(c: *LuaLoaderCCtx) ?*LoadedRow {
    const row = c.loader.nextRow() catch |err| {
        logger.err("Error getting next row: {}", .{err});
        return @ptrFromInt(0);
    };
    if (row == null) {
        return @ptrFromInt(0);
    }
    return row;
}

pub export fn ultarReclaimRow(c: *LuaLoaderCCtx, c_row: *LoadedRow) void {
    c.loader.reclaimRow(c_row);
}
