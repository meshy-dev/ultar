const std = @import("std");
const zlua = @import("zlua");
const Lua = @import("zlua").Lua;
const lua_rt = @import("lua_rt.zig");
const dataloader = @import("dataloader.zig");
const LoaderCtx = dataloader.LoaderCtx;

const logger = std.log.scoped(.lua_dataloader);

// ---------------------------------------------------------------------------
// LuaJIT-GC64 47-bit pointer constraint (Linux only)
//
// LuaJIT in GC64 mode packs GC pointers into 47-bit fields and rejects any
// allocation whose address has bit 47 (or higher) set — `lua_newstate` frees
// the GG_State and returns NULL, which surfaces as `error.OutOfMemory`. On
// Linux/aarch64 with 48-bit virtual addressing, the kernel routinely hands
// userspace mappings above 0x0000_8000_0000_0000, so `smp_allocator` triggers
// this rejection. (macOS keeps user VA ≤ 47 bits on both x86_64 and arm64,
// and we don't ship LuaJIT there anyway.) LuaJIT works around it in
// `lj_alloc.c` by probing mmap with hint addresses; we reproduce that
// probing as a Zig page-level allocator and compose a `DebugAllocator` on
// top of it for the per-state general-purpose layer.
// ---------------------------------------------------------------------------

const needs_lua_low_mmap = zlua.lang == .luajit and @import("builtin").os.tag == .linux;

const lj_mbits: u6 = 47; // GC64 packs pointers into 47 bits.
const lj_probe_max: u32 = 30;
const lj_probe_linear: u32 = 5;
const lj_probe_lower: usize = 0x4000;

var lj_hint_addr: std.atomic.Value(usize) = .init(0);
var lj_prng_state: std.atomic.Value(u64) = .init(0x9E3779B97F4A7C15);

fn ljNextRandom() u64 {
    // SplitMix64; doesn't need to be high-quality, just spread enough across
    // the low 47 bits to escape unmappable regions after linear probing fails.
    var x = lj_prng_state.fetchAdd(0x9E3779B97F4A7C15, .monotonic);
    x = (x ^ (x >> 30)) *% 0xBF58476D1CE4E5B9;
    x = (x ^ (x >> 27)) *% 0x94D049BB133111EB;
    return x ^ (x >> 31);
}

fn ljFits47(addr: usize, size: usize) bool {
    return (addr >> lj_mbits) == 0 and ((addr + size) >> lj_mbits) == 0;
}

const lj_low_page_vtable: std.mem.Allocator.VTable = .{
    .alloc = ljLowAlloc,
    .resize = std.mem.Allocator.noResize,
    .remap = std.mem.Allocator.noRemap,
    .free = ljLowFree,
};

/// Page-level allocator that mmaps memory guaranteed to fit in 47 bits.
/// Backs the per-state `DebugAllocator` we hand to LuaJIT.
pub const lj_low_page_allocator: std.mem.Allocator = .{
    .ptr = undefined,
    .vtable = &lj_low_page_vtable,
};

fn ljLowAlloc(_: *anyopaque, n: usize, alignment: std.mem.Alignment, _: usize) ?[*]u8 {
    const page_size = std.heap.pageSize();
    // DebugAllocator asks for page_size (≥128 KB) alignment for its slab
    // pages, so honor whatever the caller wants — and round len up to the
    // same boundary so mmap returns a chunk we can hand back as a slot.
    const align_bytes = @max(alignment.toByteUnits(), page_size);
    const align_mask = ~(align_bytes - 1);
    const len = std.mem.alignForward(usize, @max(n, page_size), align_bytes);

    // Strategy mirrors lj_alloc.c's mmap_probe: pick a hint inside the low
    // 47-bit window, MAP_FIXED_NOREPLACE so the kernel either honors the hint
    // exactly or fails (vs plain mmap which silently relocates to wherever).
    // Seed with last successful tail so contiguous allocations cluster.
    var hint = lj_hint_addr.load(.monotonic) & align_mask;
    if (hint == 0 or !ljFits47(hint, len)) {
        hint = pickRandomHint(align_mask, len);
    }

    var retry: u32 = 0;
    while (retry < lj_probe_max) : (retry += 1) {
        const hint_ptr: [*]align(std.heap.page_size_min) u8 = @ptrFromInt(hint);
        if (std.posix.mmap(
            hint_ptr,
            len,
            .{ .READ = true, .WRITE = true },
            .{ .TYPE = .PRIVATE, .ANONYMOUS = true, .FIXED_NOREPLACE = true },
            -1,
            0,
        )) |slice| {
            const addr = @intFromPtr(slice.ptr);
            // FIXED_NOREPLACE guarantees `addr == hint` on success.
            std.debug.assert(addr == hint);
            if (ljFits47(addr, len)) {
                lj_hint_addr.store(addr + len, .monotonic);
                return slice.ptr;
            }
            std.posix.munmap(slice); // Shouldn't happen, but be defensive.
        } else |err| switch (err) {
            error.OutOfMemory => return null,
            error.MappingAlreadyExists => {}, // try a new hint
            else => return null,
        }

        // Pick the next hint. Linear probe forward in `len`-sized strides for
        // a few rounds (cheap if the window has free space nearby), then
        // random anywhere in the low 47-bit space.
        if (retry < lj_probe_linear) {
            hint +%= @max(len, 0x1000000);
            hint &= align_mask;
            if (!ljFits47(hint, len)) hint = pickRandomHint(align_mask, len);
        } else {
            hint = pickRandomHint(align_mask, len);
        }
    }
    return null;
}

fn pickRandomHint(align_mask: usize, len: usize) usize {
    while (true) {
        const candidate = ljNextRandom() & ((@as(usize, 1) << lj_mbits) - 1) & align_mask;
        if (candidate >= lj_probe_lower and ljFits47(candidate, len)) return candidate;
    }
}

fn ljLowFree(_: *anyopaque, memory: []u8, alignment: std.mem.Alignment, _: usize) void {
    const page_size = std.heap.pageSize();
    const align_bytes = @max(alignment.toByteUnits(), page_size);
    const len = std.mem.alignForward(usize, @max(memory.len, page_size), align_bytes);
    std.posix.munmap(@alignCast(memory.ptr[0..len]));
}

// Per-Lua-state GPA backed by `lj_low_page_allocator`. `void` on platforms
// where the low-mmap dance isn't needed so the field disappears entirely.
const LuaGpa = if (needs_lua_low_mmap)
    std.heap.DebugAllocator(.{ .thread_safe = false })
else
    void;

pub const LuaLoaderSpec = extern struct {
    src: [*c]const u8,
    shard_list: [*c]const [*c]const u8,
    num_shards: c_uint,
    rank: c_uint,
    world_size: c_uint,
    debug: bool,
    config_keys: [*c]const [*c]const u8 = null,
    config_values: [*c]const [*c]const u8 = null,
    config_count: c_uint = 0,
};

const c_u8ptr = [*c]const u8;

pub const LoadedRow = extern struct {
    keys: [*c]c_u8ptr = null,
    data: [*c]c_u8ptr = null,
    sizes: [*c]u64 = null,
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
            file_handle: u64,
        },
        add_entry: struct {
            // args
            key: [:0]const u8,
            offset: u64,
            size: u32,
            file_handle: u64,
            // state
            entry: ?*Row.Entry = null,
        },
        generic: struct {},
    };

    alloc: std.mem.Allocator,
    threaded: std.Io.Threaded = undefined,
    io: std.Io = undefined,
    loader: LoaderCtx = undefined,
    // Linux+LuaJIT only: per-state GPA over `lj_low_page_allocator` to keep
    // every GC pointer within 47 bits. Compiles out elsewhere.
    lua_gpa: LuaGpa = if (needs_lua_low_mmap) .init else {},
    lua: *Lua = undefined,
    rt: lua_rt.LuaRt = undefined,

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

    // Guards free_list and num_floating_rows; reclaimRow runs on the Python thread.
    row_buf_mutex: std.Io.Mutex = .init,
    free_list: std.DoublyLinkedList = .{},
    num_floating_rows: usize = 0,

    load_rid_to_row: std.AutoArrayHashMapUnmanaged(u64, *Row),

    last_instant: std.Io.Clock.Timestamp,
    last_log_instant: std.Io.Clock.Timestamp,
    mbps_smoothed: f64 = 0.0,
    mbps_period_max: f64 = 0.0,
    samples_count: u64 = 0,

    const max_floating_rows: usize = 16;

    fn gOpenFile(lua: *Lua) !i32 {
        const loader = try lua.toUserdata(Self, 1);
        // Safe to borrow: Lua yields immediately after, keeping the string pinned.
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
        const handle: u64 = try lua_rt.toUnsigned64(lua, 2);
        loader.u_yielded_from = .{
            .close_file = .{
                .file_handle = handle,
            },
        };
        return 0;
    }

    fn gAddEntry(lua: *Lua) !i32 {
        const loader = try lua.toUserdata(Self, 1);
        const handle: u64 = try lua_rt.toUnsigned64(lua, 2);
        // Safe to borrow: Lua yields immediately after, keeping the string pinned.
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

        self.row_buf_mutex.lockUncancelable(self.io);
        const free_node_opt = self.free_list.popFirst();
        self.row_buf_mutex.unlock(self.io);

        if (free_node_opt) |free_node| {
            var row: *Row = @fieldParentPtr("node", free_node);
            try row.reset();
            self.in_progress_row = row;
        } else {
            self.in_progress_row = try Row.initAlloc(self.alloc);
        }
    }

    /// Pops the stack top and returns a registry reference to it.
    fn luaPopAndRef(self: *Self) !i32 {
        if (zlua.lang == .luau) {
            const ref = self.lua.ref(-1);
            self.lua.pop(1); // pop the value
            return ref;
        } else {
            return self.lua.ref(zlua.registry_index);
        }
    }

    /// Returns a registry reference to `table[field_name]`, erroring if it is not a function.
    fn getFieldAsFuncRef(self: *Self, table: i32, field_name: [:0]const u8) !i32 {
        _ = self.lua.getField(table, field_name);
        if (self.lua.isFunction(-1)) {
            return self.luaPopAndRef();
        } else {
            const type_name = self.lua.typeName(self.lua.typeOf(-1));
            self.lua.pop(1);
            logger.err("Expected {s} to be a function, got {s}", .{ field_name, type_name });
            return error.LuaError;
        }
    }

    fn printLuaErr(self: *Self, err: anyerror) anyerror {
        return lua_rt.printLuaErr(self.lua, err);
    }

    /// Resumes the generator coroutine, or short-circuits if it is completed or has an unresolved yield.
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
            .yield => return .yield,
        }
    }

    pub fn nextRow(self: *Self) !?*LoadedRow {
        var wait_time_ns: u64 = 1_024; // ~1us
        const wait_time_cap: u64 = 1 << 24; // ~16ms
        while (true) {
            const n = self.queue_len;
            if (n < self.queue_size_rows) {
                if (try self.resumeGenerator() == .ok and n == 0) {
                    return null;
                }
                wait_time_ns = @max(wait_time_ns / 2, 1);
            } else {
                wait_time_ns = @min(wait_time_ns * 2, wait_time_cap);
            }

            if (self.u_yielded_from != null) {
                switch (self.u_yielded_from.?) {
                    .open_file => |*f| {
                        if (f.sent_rid == 0) {
                            if (self.loader.trySend(.{ .open_file = .{ .file_path = f.file } })) |rid| {
                                f.sent_rid = rid;
                            }
                        }
                        // Cleared in the response handler once the file handle arrives.
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

            while (self.loader.tryRecv()) |resp| {
                // FIXME: make some of these errors recoverable
                const payload = try resp.payload;

                switch (payload) {
                    .open_file => |f| {
                        if (self.u_yielded_from == null or self.u_yielded_from.? != .open_file) {
                            return error.UnexpectedOpenFileResponse;
                        }
                        lua_rt.pushUnsigned64(self.lua, @bitCast(f));
                        self.u_resume_nargs = 1;
                        self.u_yielded_from = null;
                    },
                    .read_block => {
                        const kv = self.load_rid_to_row.fetchSwapRemove(resp.request_id) orelse @panic("read_block rid not found in map");
                        kv.value.num_fullfilled += 1;
                    },
                }
            }

            if (self.queue.first) |f_node| {
                const first: *Row = @fieldParentPtr("node", f_node);

                logger.debug("Q len: {}, first: fullfilled = {}, entries = {}", .{ self.queue_len, first.num_fullfilled, first.entries.items.len });

                if (first.num_fullfilled == first.entries.items.len) {
                    const node = self.queue.popFirst() orelse unreachable;
                    self.queue_len -= 1;

                    {
                        self.row_buf_mutex.lockUncancelable(self.io);
                        defer self.row_buf_mutex.unlock(self.io);
                        self.num_floating_rows += 1;
                        if (self.num_floating_rows > Self.max_floating_rows) {
                            std.debug.panic("Too many floating (owned by client) rows > max: {}", .{Self.max_floating_rows});
                        }
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

                    const now = std.Io.Clock.Timestamp.now(self.io, .awake);
                    const delta_ns: i96 = self.last_instant.durationTo(now).raw.nanoseconds;
                    self.last_instant = now;
                    const mbps = @as(f64, @floatFromInt(bytes)) * 1e-6 / (@as(f64, @floatFromInt(delta_ns)) * 1e-9);

                    self.samples_count += 1;
                    const smoothing_samples = @min(self.samples_count, 100);
                    const alpha = 1.0 / @as(f64, @floatFromInt(smoothing_samples));
                    self.mbps_smoothed = alpha * mbps + (1.0 - alpha) * self.mbps_smoothed;

                    self.mbps_period_max = @max(self.mbps_period_max, mbps);

                    const since_last_log_ns: i96 = self.last_log_instant.durationTo(now).raw.nanoseconds;
                    if (since_last_log_ns >= 60 * std.time.ns_per_s) {
                        logger.info("{d:.1} MBytes/s (Period max: {d:.1})", .{ self.mbps_smoothed, self.mbps_period_max });
                        self.last_log_instant = now;
                        self.mbps_period_max = 0.0;
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
            std.Io.sleep(self.io, .fromNanoseconds(@intCast(wait_time_ns)), .awake) catch {};
        }
    }

    pub fn reclaimRow(self: *Self, c_row: *LoadedRow) void {
        const row: *Row = @fieldParentPtr("ext_row", c_row);

        self.row_buf_mutex.lockUncancelable(self.io);
        defer self.row_buf_mutex.unlock(self.io);

        self.free_list.append(&row.node);
        self.num_floating_rows -= 1;
    }

    /// `require("ultar.loader")` body; returns the loader interface table. `self` is the closure upvalue.
    fn loaderModuleLoader(lua: *Lua) !i32 {
        const self = try lua.toUserdata(Self, Lua.upvalueIndex(1));

        lua.createTable(0, 5); // [+p] module table

        lua.pushLightUserdata(self); // [+p]
        lua.setField(-2, "c_loader"); // pop

        try Self.wrapCoyield(lua, "loader_open_file", Self.gOpenFile); // [+p]
        lua.setField(-2, "open_file"); // pop
        try Self.wrapCoyield(lua, "loader_close_file", Self.gCloseFile); // [+p]
        lua.setField(-2, "close_file"); // pop
        try Self.wrapCoyield(lua, "loader_add_entry", Self.gAddEntry); // [+p]
        lua.setField(-2, "add_entry"); // pop
        try Self.wrapCoyield(lua, "loader_finish_row", Self.gFinishRow); // [+p]
        lua.setField(-2, "finish_row"); // pop

        return 1;
    }

    /// Installs the loader module under `package.preload["ultar.loader"]` with `self` captured as an upvalue.
    fn registerLoaderModule(self: *Self) !void {
        _ = self.lua.getGlobal("package"); // [+p]
        _ = self.lua.getField(-1, "preload"); // [+p]

        self.lua.pushLightUserdata(self); // [+p] upvalue
        self.lua.pushClosure(zlua.wrap(Self.loaderModuleLoader), 1); // pop upvalue, push closure

        self.lua.setField(-2, "ultar.loader"); // pop closure
        self.lua.pop(2); // pop preload, package
    }

    fn initLua(self: *Self, spec: LuaLoaderSpec) !void {
        self.lua.openLibs();
        // `self` is heap-allocated, so `&self.rt` is a stable pointer for the
        // Lua state's lifetime.
        self.rt.init(self.lua, self.io) catch |err| return self.printLuaErr(err);

        self.registerLoaderModule() catch |err| return self.printLuaErr(err);

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

        // Loader script must return a table of { init_ctx, row_generator }.
        self.lua.protectedCall(.{ .results = 1 }) catch |err| return self.printLuaErr(err);

        const table = self.lua.getTop();
        if (!self.lua.isTable(table)) {
            const type_name = self.lua.typeName(self.lua.typeOf(table));
            logger.err("Expected spec result to be a table, got {s}", .{type_name});
            return error.LuaError;
        }

        self.u_loader_fn.init_ctx = try self.getFieldAsFuncRef(table, "init_ctx");
        self.u_loader_fn.row_generator = try self.getFieldAsFuncRef(table, "row_generator");

        self.lua.pop(1); // drop the table

        // init_ctx(rank, world_size, config)
        _ = self.lua.getIndexRaw(zlua.registry_index, self.u_loader_fn.init_ctx);
        lua_rt.pushUnsigned(self.lua, @intCast(spec.rank));
        lua_rt.pushUnsigned(self.lua, @intCast(spec.world_size));

        self.lua.createTable(@intCast(spec.config_count), 0); // [+p] config
        if (spec.config_keys != null and spec.config_values != null) {
            for (0..spec.config_count) |i| {
                const key: [*:0]const u8 = @ptrCast(spec.config_keys[i]);
                const value: [*:0]const u8 = @ptrCast(spec.config_values[i]);
                _ = self.lua.pushString(std.mem.span(value)); // [+p]
                self.lua.setField(-2, std.mem.span(key)); // pop value
            }
        }

        self.lua.protectedCall(.{ .args = 3, .results = 1 }) catch |err| return self.printLuaErr(err);
        self.u_ctx = self.luaPopAndRef() catch |err| return self.printLuaErr(err);

        // Prime the generator coroutine; first resume calls row_generator(u_ctx).
        _ = self.lua.getIndexRaw(zlua.registry_index, self.u_loader_fn.row_generator); // [+p]
        _ = self.lua.getIndexRaw(zlua.registry_index, self.u_ctx); // [+p]
        self.u_resume_nargs = 1;
    }

    pub fn init(spec: LuaLoaderSpec, alloc: std.mem.Allocator) !*Self {
        var self = try alloc.create(Self);
        errdefer alloc.destroy(self);

        // No `std.process.Init` at the Python extension entry point; own the Io ourselves.
        self.threaded = .init(alloc, .{});
        errdefer self.threaded.deinit();
        self.io = self.threaded.io();

        const now = std.Io.Clock.Timestamp.now(self.io, .awake);
        self.alloc = alloc;
        self.loader = undefined;
        self.lua = undefined;
        self.u_loader_fn = .{};
        self.u_ctx = 0;
        self.u_ctx_funcs_table = 0;
        self.u_resume_nargs = 0;
        self.u_yielded_from = null;
        self.u_completed = false;
        self.queue_size_rows = 4;
        self.in_progress_row = null;
        self.queue = .{};
        self.queue_len = 0;
        self.row_buf_mutex = .init;
        self.free_list = .{};
        self.num_floating_rows = 0;
        self.load_rid_to_row = try std.AutoArrayHashMapUnmanaged(u64, *Row).init(alloc, &.{}, &.{});
        self.last_instant = now;
        self.last_log_instant = now;
        self.mbps_smoothed = 0.0;
        self.mbps_period_max = 0.0;
        self.samples_count = 0;
        errdefer self.load_rid_to_row.deinit(self.alloc);

        try self.newInprogressRow();
        errdefer {
            const row = self.in_progress_row orelse unreachable;
            row.deinit();
            self.alloc.destroy(row);
            self.in_progress_row = null;
        }

        try self.loader.initInPlace(alloc);
        errdefer self.loader.deinit();
        try self.loader.start(self.io);

        const lua_alloc = if (needs_lua_low_mmap) blk: {
            self.lua_gpa = .init;
            self.lua_gpa.backing_allocator = lj_low_page_allocator;
            break :blk self.lua_gpa.allocator();
        } else alloc;
        self.lua = try Lua.init(lua_alloc);
        errdefer self.lua.deinit();
        try self.initLua(spec);

        return self;
    }

    pub fn deinit(self: *Self) void {
        self.load_rid_to_row.deinit(self.alloc);
        self.loader.deinit();
        self.lua.deinit();
        // Tear down the low-mmap GPA *after* lua.deinit has freed all
        // remaining GC objects back through it.
        if (needs_lua_low_mmap) _ = self.lua_gpa.deinit();

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

        self.threaded.deinit();
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
    return createLuaLoader(spec) catch |err| {
        logger.err("createLuaLoader failed: {}", .{err});
        std.debug.dumpCurrentStackTrace(.{});
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
