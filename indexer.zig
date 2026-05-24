const std = @import("std");
const clap = @import("clap");
const xev = @import("xev");

const tardefs = @import("tardefs.zig");
const scanners = @import("scanners.zig");
const M = @import("msgpack");
const OStream = @import("XevOstream.zig");

const index_ext = "utix";

const logger = std.log.scoped(.indexer);

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;
    const io = init.io;

    var stderr_buffer: [1024]u8 = undefined;
    var stderr_writer = std.Io.File.stderr().writer(io, &stderr_buffer);
    const stderr = &stderr_writer.interface;
    defer stderr.flush() catch @panic("Error flushing stderr");

    const params = comptime clap.parseParamsComptime(
        \\-h, --help           Display this help and exit.
        \\--fmt <STR>          Output format (msgpack / jsonl).
        \\-f, --file <FILE>... Tar file(s) to index (compatibility; positional FILEs are preferred).
        \\--meta-rule <STR>... Metadata rule(s) "KEY:QLIST".
        \\<FILE>...            Tar file(s) to index.
        \\
    );

    const parsers = comptime .{
        .STR = clap.parsers.string,
        .FILE = clap.parsers.string,
    };

    var diag = clap.Diagnostic{};
    var res = clap.parse(clap.Help, &params, parsers, init.minimal.args, .{
        .diagnostic = &diag,
        .allocator = allocator,
    }) catch |err| {
        diag.report(stderr, err) catch {};
        return err;
    };
    defer res.deinit();

    if (res.args.help != 0)
        return clap.help(stderr, clap.Help, &params, .{});

    var tarfile_list: std.ArrayListUnmanaged([]const u8) = .empty;
    defer tarfile_list.deinit(allocator);
    try tarfile_list.appendSlice(allocator, res.args.file);
    try tarfile_list.appendSlice(allocator, res.positionals[0]);
    const tarfiles = tarfile_list.items;

    const num_tarfiles = tarfiles.len;
    var indexers = try allocator.alloc(Indexer, num_tarfiles);
    // `allocator.alloc` returns uninitialized memory (0xaa poison under the
    // debug allocator on Linux, whatever c_allocator hands back elsewhere).
    // Zero the slice up front so the cleanup defer below can treat
    // `fs_file == null` as the real init flag: a zero-init Indexer has a null
    // optional file and empty arena lists, both of which the partial-cleanup
    // path handles correctly without touching uninitialized fields.
    @memset(std.mem.sliceAsBytes(indexers), 0);
    defer {
        for (indexers) |*t| {
            if (t.fs_file != null) {
                t.state.deinit();
                t.deinit(io);
            }
        }
        allocator.free(indexers);
    }

    const fmt = if (res.args.fmt) |fmt_str| (std.meta.stringToEnum(WdsIndexingState.SerializationFormat, fmt_str) orelse {
        logger.err("Unrecognized format: {s}", .{fmt_str});
        return clap.help(stderr, clap.Help, &params, .{});
    }) else .msgpack;

    var cli_arena = std.heap.ArenaAllocator.init(allocator);
    defer cli_arena.deinit();
    const cli_alloc = cli_arena.allocator();

    const meta_rule_args: []const []const u8 = @field(res.args, "meta-rule");
    var rule_list: std.ArrayListUnmanaged(WdsIndexingState.Rule) = .empty;
    for (meta_rule_args) |rule_str| {
        const colon = std.mem.indexOfScalar(u8, rule_str, ':') orelse {
            logger.err("Invalid --meta-rule format (expected KEY:QLIST): {s}", .{rule_str});
            diag.report(stderr, error.InvalidRuleFormat) catch {};
            return error.InvalidRuleFormat;
        };
        const meta_key_src = rule_str[0..colon];
        for (rule_list.items) |existing| {
            if (std.mem.eql(u8, existing.meta_key, meta_key_src)) {
                logger.err("Duplicate --meta-rule key: {s}", .{meta_key_src});
                return error.DuplicateMetaRule;
            }
        }
        const meta_key = try cli_alloc.dupe(u8, meta_key_src);
        const qpart = rule_str[colon + 1 ..];
        var qlist: std.ArrayListUnmanaged([]const u8) = .empty;
        var qiter = std.mem.splitScalar(u8, qpart, ';');
        while (qiter.next()) |q| if (q.len > 0) {
            const qd = try cli_alloc.dupe(u8, q);
            try qlist.append(cli_alloc, qd);
        };
        const queries = try qlist.toOwnedSlice(cli_alloc);
        try rule_list.append(cli_alloc, .{ .meta_key = meta_key, .queries = queries });
    }
    const rules = try rule_list.toOwnedSlice(cli_alloc);

    // kqueue (macOS) needs a thread pool to service regular-file I/O,
    // otherwise every read returns EPERM. io_uring on Linux handles file
    // I/O in-kernel, so skip the pool there to avoid idle worker threads.
    const needs_thread_pool = @import("builtin").os.tag != .linux;
    var thread_pool: if (needs_thread_pool) xev.ThreadPool else void =
        if (needs_thread_pool) .init(.{}) else {};
    defer if (needs_thread_pool) {
        thread_pool.shutdown();
        thread_pool.deinit();
    };

    var loop = try xev.Loop.init(.{
        .thread_pool = if (needs_thread_pool) &thread_pool else null,
    });
    defer loop.deinit();

    for (tarfiles, 0..) |fp, i| {
        const indexer = &indexers[i];
        const out_fp = try std.mem.join(allocator, ".", &[_][]const u8{ fp, index_ext });
        defer allocator.free(out_fp);
        const out_file = std.Io.Dir.cwd().createFile(io, out_fp, .{ .truncate = true }) catch |err| {
            logger.err("Error opening output file {s}: {}", .{ out_fp, err });
            return err;
        };
        const state = try WdsIndexingState.init(allocator, io, &loop, out_file, fmt, rules, fp);
        indexer.* = try Indexer.initFp(io, state, fp);
        indexer.enqueueRead(&loop);
    }

    try loop.run(.until_done);
}

pub const IndexMetadataError = error{
    RowTooLarge,
    EntryTooLarge,
    TooManyRows,
    TooManyColumns,
};

pub const Indexer = scanners.TarFileScanner(WdsIndexingState, .{
    .entry_cb = WdsIndexingState.scannedEntryCb,
    .error_cb = WdsIndexingState.errorCb,
    .done_cb = WdsIndexingState.doneCb,
});

pub const WdsIndexingState = struct {
    const Self = @This();

    const Entry = struct {
        key: []const u8,
        offset_from_base: u32 = 0,
        size: u32 = 0,
    };

    const Row = struct {
        iidx: usize = 0,
        offset: usize = 0,
        str_idx: []const u8,
        keys: []const []const u8,
        offsets: []const u32,
        sizes: []const u32,
    };

    const Rule = struct {
        meta_key: []const u8,
        queries: []const []const u8,
    };

    const MetaEntry = struct {
        query: []const u8,
        value: std.json.Value,
    };

    pub const SerializationFormat = enum { msgpack, jsonl };

    const MsgpackSer = M.Packer;
    const JsonSer = std.json.Stringify;
    const Packer = union(enum) {
        msgpack: MsgpackSer,
        jsonl: JsonSer,
    };

    fn getValueForQuery(root: std.json.Value, query: []const u8) std.json.Value {
        if (query.len == 0) return root;
        var current = root;
        var rest = if (query.len > 0 and query[0] == '.') query[1..] else query;
        while (rest.len > 0) {
            const dot_pos = std.mem.indexOfScalar(u8, rest, '.');
            var seg = if (dot_pos) |d| rest[0..d] else rest;
            rest = if (dot_pos) |d| rest[d + 1 ..] else "";
            var key = seg;
            var arr_idx: ?usize = null;
            if (std.mem.indexOfScalar(u8, seg, '[')) |b| {
                key = seg[0..b];
                if (std.mem.indexOfScalar(u8, seg, ']')) |c| {
                    const idxs = seg[b + 1 .. c];
                    arr_idx = std.fmt.parseInt(usize, idxs, 10) catch return .null;
                } else return .null;
            }
            if (key.len > 0) {
                if (current != .object) return .null;
                current = current.object.get(key) orelse return .null;
            }
            if (arr_idx) |i| {
                if (current != .array) return .null;
                if (i >= current.array.items.len) return .null;
                current = current.array.items[i];
            }
        }
        return current;
    }

    fn emitJsonValue(self: *Self, pack: *MsgpackSer, v: std.json.Value) !void {
        switch (v) {
            .null => try pack.addNil(),
            .bool => |b| try pack.addBool(b),
            .integer => |i| try pack.addInt(i64, i),
            .float => |f| try pack.addFloat(f64, f),
            .number_string => |s| try pack.addStr(s),
            .string => |s| try pack.addStr(s),
            .array => |a| {
                try pack.beginArray(a.items.len);
                for (a.items) |item| try self.emitJsonValue(pack, item);
            },
            .object => |o| {
                try pack.beginMap(o.count());
                var it = o.iterator();
                while (it.next()) |entry| {
                    try pack.addStr(entry.key_ptr.*);
                    try self.emitJsonValue(pack, entry.value_ptr.*);
                }
            },
        }
    }

    fn emitJsonValueJsonl(self: *Self, j: *JsonSer, v: std.json.Value) !void {
        switch (v) {
            .null => try j.write(null),
            .bool => |b| try j.write(b),
            .integer => |i| try j.write(i),
            .float => |f| try j.write(f),
            .number_string => |s| try j.write(s),
            .string => |s| try j.write(s),
            .array => |a| {
                try j.beginArray();
                for (a.items) |item| {
                    try self.emitJsonValueJsonl(j, item);
                }
                try j.endArray();
            },
            .object => |o| {
                try j.beginObject();
                var it = o.iterator();
                while (it.next()) |entry| {
                    try j.objectField(entry.key_ptr.*);
                    try self.emitJsonValueJsonl(j, entry.value_ptr.*);
                }
                try j.endObject();
            },
        }
    }

    gpa: std.mem.Allocator,
    io: std.Io,
    rows: usize = 0,

    ostream: OStream,

    current_row_str_idx_buf: [1024]u8 = undefined,
    current_row_str_idx: ?[]const u8 = null,
    current_row_base: usize = 0,
    current_row_has_meta_match: bool = false,

    fmt: SerializationFormat,

    rules: []const Rule = &.{},
    input_path: ?[]const u8 = null,

    /// Atomic monotonic tar-input byte offset; null disables reporting.
    progress_ptr: ?*usize = null,

    /// Set once by `doneCb`; null disables completion signaling.
    done_event_ptr: ?*std.Io.Event = null,

    /// Fired by `doneCb` before `done_event_ptr` is set.
    done_notify_cb: ?*const fn (ctx: *anyopaque) void = null,
    done_notify_ctx: ?*anyopaque = null,

    /// Guards `finalize` against re-entry.
    finalized: bool = false,

    row_buf: std.ArrayListUnmanaged(Entry),
    row_arena: std.heap.ArenaAllocator,
    meta_buf: std.ArrayListUnmanaged(MetaEntry),

    pub fn init(alloc: std.mem.Allocator, io: std.Io, loop: *xev.Loop, output_file: std.Io.File, fmt: SerializationFormat, rules: []const Rule, input_path: ?[]const u8) !Self {
        var ostream = try OStream.init(loop, output_file);
        errdefer ostream.deinit(io);

        var row_buf = try std.ArrayListUnmanaged(Entry).initCapacity(alloc, 256);
        errdefer row_buf.deinit(alloc);

        var meta_buf = try std.ArrayListUnmanaged(MetaEntry).initCapacity(alloc, 16);
        errdefer meta_buf.deinit(alloc);

        return .{
            .gpa = alloc,
            .io = io,
            .ostream = ostream,
            .row_buf = row_buf,
            .row_arena = std.heap.ArenaAllocator.init(alloc),
            .meta_buf = meta_buf,
            .fmt = fmt,
            .rules = rules,
            .input_path = input_path,
        };
    }

    pub fn deinit(self: *Self) void {
        self.ostream.deinit(self.io);
        self.row_buf.deinit(self.gpa);
        self.row_arena.deinit();
        self.meta_buf.deinit(self.gpa);
    }

    fn writeRowMsgPack(self: *Self) !void {
        const writer = &self.ostream.interface;
        var pack = MsgpackSer{ .writer = writer };

        const items = self.row_buf.items;
        const num_entries = items.len;
        const has_meta = self.meta_buf.items.len > 0;
        const total = std.meta.fields(Row).len + @as(usize, @intFromBool(has_meta));
        try pack.beginMap(total);
        {
            try pack.addStr("iidx");
            try pack.addInt(usize, self.rows);
            try pack.addStr("offset");
            try pack.addInt(usize, self.current_row_base);
            try pack.addStr("str_idx");
            try pack.addStr(self.current_row_str_idx.?);
            try pack.addStr("keys");
            try pack.beginArray(num_entries);
            for (self.row_buf.items) |e| try pack.addStr(e.key);
            try pack.addStr("offsets");
            try pack.beginArray(num_entries);
            for (self.row_buf.items) |e| try pack.addInt(u32, e.offset_from_base);
            try pack.addStr("sizes");
            try pack.beginArray(num_entries);
            for (self.row_buf.items) |e| try pack.addInt(u32, e.size);
            if (has_meta) {
                try pack.addStr("metadata");
                try pack.beginMap(self.meta_buf.items.len);
                for (self.meta_buf.items) |m| {
                    try pack.addStr(m.query);
                    try self.emitJsonValue(&pack, m.value);
                }
            }
        }
    }

    fn writeRowJsonl(self: *Self) !void {
        const writer = &self.ostream.interface;
        var json = std.json.Stringify{ .writer = writer };

        const items = self.row_buf.items;
        try json.beginObject();
        try json.objectField("iidx");
        try json.write(self.rows);
        try json.objectField("offset");
        try json.write(self.current_row_base);
        try json.objectField("str_idx");
        try json.write(self.current_row_str_idx.?);
        try json.objectField("keys");
        try json.beginArray();
        for (items) |e| try json.write(e.key);
        try json.endArray();
        try json.objectField("offsets");
        try json.beginArray();
        for (items) |e| try json.write(e.offset_from_base);
        try json.endArray();
        try json.objectField("sizes");
        try json.beginArray();
        for (items) |e| try json.write(e.size);
        try json.endArray();
        const has_meta = self.meta_buf.items.len > 0;
        if (has_meta) {
            try json.objectField("metadata");
            try json.beginObject();
            for (self.meta_buf.items) |m| {
                try json.objectField(m.query);
                try self.emitJsonValueJsonl(&json, m.value);
            }
            try json.endObject();
        }
        try json.endObject();

        try writer.writeByte('\n');
    }

    pub fn writeRow(self: *Self) !void {
        if (self.current_row_str_idx == null) return;
        if (self.rows > std.math.maxInt(i32)) {
            return IndexMetadataError.TooManyRows;
        }

        switch (self.fmt) {
            .msgpack => try self.writeRowMsgPack(),
            .jsonl => try self.writeRowJsonl(),
        }

        self.rows += 1;
    }

    pub fn pushEntry(self: *Self, header: *tardefs.TarHeader, offset: usize, size: usize) !void {
        if (size > std.math.maxInt(std.meta.fieldInfo(Entry, .size).type)) {
            return IndexMetadataError.EntryTooLarge;
        }

        const name = header.name[0..(std.mem.indexOfScalar(u8, &header.name, 0) orelse header.name.len)];
        const first_slash = std.mem.indexOfScalar(u8, name, std.fs.path.sep) orelse 0;
        const first_ext = first_slash + (std.mem.indexOfScalar(u8, header.name[first_slash..], '.') orelse (name.len - first_slash));
        const row_str_idx = name[0..first_ext];
        const entry_key = name[first_ext..];

        if (self.current_row_str_idx == null or !std.mem.eql(u8, row_str_idx, self.current_row_str_idx.?)) {
            try self.writeRow();

            std.mem.copyForwards(u8, &self.current_row_str_idx_buf, row_str_idx);
            self.current_row_str_idx = self.current_row_str_idx_buf[0..row_str_idx.len];
            self.row_buf.clearRetainingCapacity();
            self.meta_buf.clearRetainingCapacity();
            _ = self.row_arena.reset(.retain_capacity);
            self.current_row_base = offset;
            self.current_row_has_meta_match = false;
        }

        if (self.rules.len > 0) {
            for (self.rules) |r| {
                if (std.mem.eql(u8, r.meta_key, entry_key)) {
                    self.current_row_has_meta_match = true;
                    blk: {
                        const path = self.input_path orelse break :blk;
                        const file = std.Io.Dir.cwd().openFile(self.io, path, .{ .mode = .read_only }) catch |e| {
                            logger.warn("meta open failed for {s}: {}", .{ path, e });
                            break :blk;
                        };
                        defer file.close(self.io);
                        const blob = self.row_arena.allocator().alloc(u8, size) catch break :blk;
                        const got = std.Io.File.readPositionalAll(file, self.io, blob, offset) catch |e| {
                            logger.warn("meta read failed for {s}: {}", .{ entry_key, e });
                            break :blk;
                        };
                        if (got != size) break :blk;
                        const root = std.json.parseFromSliceLeaky(std.json.Value, self.row_arena.allocator(), blob[0..got], .{}) catch |e| {
                            logger.warn("meta json parse failed for {s}: {}", .{ entry_key, e });
                            break :blk;
                        };
                        for (r.queries) |q| {
                            const v = getValueForQuery(root, q);
                            const qd = self.row_arena.allocator().dupe(u8, q) catch break :blk;
                            self.meta_buf.append(self.gpa, .{ .query = qd, .value = v }) catch break :blk;
                        }
                    }
                    break;
                }
            }
        }

        const offset_from_base = offset - self.current_row_base;
        if (offset_from_base > std.math.maxInt(std.meta.fieldInfo(Entry, .offset_from_base).type)) {
            return IndexMetadataError.RowTooLarge;
        }
        if (self.row_buf.items.len >= std.math.maxInt(i32)) {
            return IndexMetadataError.TooManyColumns;
        }
        try self.row_buf.append(self.gpa, .{
            .key = try self.row_arena.allocator().dupe(u8, entry_key),
            .offset_from_base = @intCast(offset_from_base),
            .size = @intCast(size),
        });
    }

    pub fn finalize(self: *Self) void {
        if (self.finalized) return;
        self.finalized = true;
        self.writeRow() catch |err| {
            logger.err("Error writing final row: {}", .{err});
        };
        self.ostream.interface.flush() catch |err| {
            logger.err("Error while flushing output file: {}", .{err});
        };
    }

    /// Scanner terminal hook; idempotent.
    pub fn doneCb(state: *Self) void {
        state.finalize();
        if (state.done_notify_cb) |cb| if (state.done_notify_ctx) |ctx| cb(ctx);
        if (state.done_event_ptr) |ev| ev.set(state.io);
    }

    pub fn scannedEntryCb(
        state: *Self,
        header: ?*tardefs.TarHeader,
        offset: usize,
        size: usize,
    ) bool {
        if (header) |h| {
            var buf: [1024]u8 = undefined;

            var prefix_len = std.mem.indexOfScalar(u8, &h.prefix, 0) orelse h.prefix.len;
            if (prefix_len > 0) {
                std.mem.copyForwards(u8, &buf, h.prefix[0..prefix_len]);
                buf[prefix_len] = std.fs.path.sep;
                prefix_len += 1;
            }
            const header_name_len = std.mem.indexOfScalar(u8, &h.name, 0) orelse h.name.len;
            if (header_name_len > 0)
                std.mem.copyForwards(u8, buf[prefix_len..], h.name[0..header_name_len]);
            const name = buf[0 .. prefix_len + header_name_len];

            logger.debug("Scanned entry: {s} {}+{}", .{ name, offset, size });

            state.pushEntry(h, offset, size) catch |err|
                switch (err) {
                    IndexMetadataError.RowTooLarge, IndexMetadataError.EntryTooLarge, IndexMetadataError.TooManyColumns => logger.warn("Skipping {s} due to {}", .{ name, err }),
                    IndexMetadataError.TooManyRows => {
                        logger.err("Too many rows! Ending indexing", .{});
                        state.finalize();
                        return false;
                    },
                    else => {
                        logger.err("Got error {}", .{err});
                        @panic("Unhandlable indexing error");
                    },
                };
            if (state.progress_ptr) |p| @atomicStore(usize, p, offset + size, .monotonic);
        } else {
            if (state.progress_ptr) |p| @atomicStore(usize, p, offset, .monotonic);
            state.finalize();
            logger.info("End of tar file, {} rows", .{state.rows});
        }
        return true;
    }

    pub fn errorCb(state: *Self, _: xev.ReadError!void) bool {
        state.finalize();
        return false;
    }
};
