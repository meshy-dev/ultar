const std = @import("std");
const clap = @import("clap");
const xev = @import("xev");

const tardefs = @import("tardefs.zig");
const scanners = @import("scanners.zig");
const M = @import("msgpack.zig");
const OStream = @import("XevOstream.zig");

const index_ext = "utix";

const logger = std.log.scoped(.indexer);

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const params = comptime clap.parseParamsComptime(
        \\-h, --help             Display this help and exit.
        \\-f, --file <str>...    Tar file(s) to index.
        \\
    );

    var diag = clap.Diagnostic{};
    var res = clap.parse(clap.Help, &params, clap.parsers.default, .{
        .diagnostic = &diag,
        .allocator = allocator,
    }) catch |err| {
        // Report useful error and exit.
        diag.report(std.io.getStdErr().writer(), err) catch {};
        return err;
    };
    defer res.deinit();

    if (res.args.help != 0)
        return clap.help(std.io.getStdErr().writer(), clap.Help, &params, .{});

    var loop = try xev.Loop.init(.{});
    defer loop.deinit();

    const Scanner = scanners.TarFileScanner(WdsIndexingState, WdsIndexingState.scannedEntryCb);
    var tarfiles = try allocator.alloc(Scanner, res.args.file.len);
    defer {
        for (tarfiles) |*t| {
            if (t.fs_file != null) {
                t.state.deinit();
                t.deinit();
            }
        }
        allocator.free(tarfiles);
    }

    for (res.args.file, 0..) |fp, i| {
        const tarfile = &tarfiles[i];
        const out_fp = try std.mem.join(allocator, ".", &[_][]const u8{ fp, index_ext });
        defer allocator.free(out_fp);
        tarfile.* = try Scanner.initFp(try WdsIndexingState.init(allocator, &loop, std.fs.cwd().createFile(out_fp, .{ .truncate = true }) catch |err| {
            logger.err("Error opening output file {s}: {}", .{ out_fp, err });
            return err;
        }), fp);
        tarfile.enqueueRead(&loop);
    }

    try loop.run(.until_done);
}

pub const IndexMetadataError = error{
    RowTooLarge,
    EntryTooLarge,
    TooManyRows,
    TooManyColumns,
};

const WdsIndexingState = struct {
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
        entries: []const Entry,
    };

    gpa: std.mem.Allocator,
    rows: usize = 0,

    ostream: OStream,

    current_row_str_idx_buf: [1024]u8 = undefined,
    current_row_str_idx: ?[]const u8 = null,
    current_row_base: usize = 0,

    row_buf: std.ArrayListUnmanaged(Entry),
    row_arena: std.heap.ArenaAllocator,

    pub fn init(alloc: std.mem.Allocator, loop: *xev.Loop, output_file: std.fs.File) !Self {
        const ostream = try OStream.init(loop, output_file);
        return .{ .gpa = alloc, .ostream = ostream, .row_buf = try std.ArrayListUnmanaged(Entry).initCapacity(alloc, 256), .row_arena = std.heap.ArenaAllocator.init(alloc) };
    }

    pub fn deinit(self: *Self) void {
        self.ostream.deinit();
        self.row_buf.deinit(self.gpa);
        self.row_arena.deinit();
    }

    pub fn writeRow(self: *Self) !void {
        if (self.current_row_str_idx == null) return;

        // Write the previous row
        var pack = M.Packer(OStream).init(&self.ostream);

        try pack.beginMap(std.meta.fields(Row).len);
        {
            try pack.addStr("iidx");
            try pack.addInt(usize, self.rows);
            try pack.addStr("offset");
            try pack.addInt(usize, self.current_row_base);
            try pack.addStr("str_idx");
            try pack.addStr(self.current_row_str_idx.?);
            try pack.addStr("entries");
            const num_entries = self.row_buf.items.len;
            try pack.beginArray(num_entries);
            for (self.row_buf.items) |entry| {
                try pack.beginMap(3);
                {
                    try pack.addStr("key");
                    try pack.addStr(entry.key);
                    try pack.addStr("offset");
                    try pack.addInt(u32, entry.offset_from_base);
                    try pack.addStr("size");
                    try pack.addInt(u32, entry.size);
                }
            }
        }

        self.rows += 1;

        if (self.rows >= std.math.maxInt(i32)) {
            return IndexMetadataError.TooManyRows;
        }
    }

    pub fn pushEntry(self: *Self, header: *tardefs.TarHeader, offset: usize, size: usize) !void {
        if (size > std.math.maxInt(std.meta.FieldType(Entry, .size))) {
            return IndexMetadataError.EntryTooLarge;
        }

        const name = header.name[0..(std.mem.indexOfScalar(u8, &header.name, 0) orelse header.name.len)];
        const first_slash = std.mem.indexOfScalar(u8, name, std.fs.path.sep) orelse 0;
        const first_ext = first_slash + (std.mem.indexOfScalar(u8, header.name[first_slash..], '.') orelse (name.len - first_slash));
        const row_str_idx = name[0..first_ext];
        const entry_key = name[first_ext..];

        if (self.current_row_str_idx == null or !std.mem.eql(u8, row_str_idx, self.current_row_str_idx.?)) {
            try self.writeRow();

            // Record new row key & start the row
            std.mem.copyForwards(u8, &self.current_row_str_idx_buf, row_str_idx);
            self.current_row_str_idx = self.current_row_str_idx_buf[0..row_str_idx.len];
            self.row_buf.clearRetainingCapacity();
            _ = self.row_arena.reset(.retain_capacity);
            self.current_row_base = offset;
        }

        // Add entry to row_buf
        const offset_from_base = offset - self.current_row_base;
        if (offset_from_base > std.math.maxInt(std.meta.FieldType(Entry, .offset_from_base))) {
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
                prefix_len += 1; // Add a separator
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
                        logger.err("Too many rows in {s} - stopping indexing", .{name});
                        state.ostream.flush();
                        return false;
                    },
                    else => {
                        logger.err("Got error {}", .{err});
                        @panic("Unhandlable indexing error");
                    },
                };
        } else {
            state.writeRow() catch |err| {
                logger.err("Error writing final row: {}", .{err});
            };
            state.ostream.flush();
            logger.info("End of tar file, {} rows", .{state.rows});
        }
        return true;
    }
};
