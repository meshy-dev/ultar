const std = @import("std");
const mem = @import("std").mem;
const xev = @import("xev");
const octal = @import("octal.zig");
const tardefs = @import("tardefs.zig");

// Tar file scanner

pub fn TarFileScanner(
    comptime State: type,
    comptime entry_cb: *const fn (
        state: *State,
        header: ?*tardefs.TarHeader,
        offset: usize,
        size: usize,
    ) bool,
    comptime error_cb: ?*const fn (state: *State, err: xev.ReadError!void) bool,
) type {
    return struct {
        const Self = @This();
        const logger = std.log.scoped(.TarFileScanner);

        const blocks: usize = 64;

        fs_file: ?std.fs.File,
        f: xev.File,
        state: State,

        completion: xev.Completion = undefined,
        read_buf: [Self.blocks * tardefs.block_size]u8 = undefined,
        offset: usize = 0,

        pub fn initFp(state: State, fp: []const u8) !Self {
            const fs_file = std.fs.cwd().openFile(fp, .{ .mode = .read_only }) catch |err| {
                std.debug.print("Failed to open file {s}. Error: {}\n", .{ fp, err });
                return err;
            };
            return try Self.init(state, fs_file);
        }

        pub fn init(state: State, fs_file: std.fs.File) !Self {
            return .{
                .fs_file = fs_file,
                .f = try xev.File.init(fs_file),
                .state = state,
            };
        }

        pub fn deinit(self: *Self) void {
            if (self.fs_file) |f| {
                f.close();
            }
        }

        pub fn readCallback(
            _self: ?*Self,
            l: *xev.Loop,
            c: *xev.Completion,
            s: xev.File,
            b: xev.ReadBuffer,
            r: xev.ReadError!usize,
        ) xev.CallbackAction {
            const self = _self orelse unreachable;
            const read_size = r catch |err| {
                logger.err("Error reading from file: {}", .{err});
                if (error_cb) |f| {
                    return if (f(&self.state, err)) .rearm else .disarm;
                }
                return .disarm;
            };
            _ = c;
            _ = s;
            _ = b;

            if (read_size < tardefs.block_size) {
                logger.err("Read {} bytes, less than block size", .{read_size});
                return .disarm;
            }
            if (read_size % tardefs.block_size != 0) {
                logger.err("Read {} bytes, not a multiple of block size", .{read_size});
                return .disarm;
            }

            var buf_offset: usize = 0;
            var zero_block_encountered: usize = 0;
            while (buf_offset < read_size) {
                const block: *tardefs.TarHeader = @ptrCast(&self.read_buf[buf_offset]);
                buf_offset += tardefs.block_size;

                if (tardefs.isZeroBlock(block) or zero_block_encountered > 0) {
                    zero_block_encountered += 1;
                    if (zero_block_encountered >= 2) {
                        logger.debug("TarFile terminated", .{});
                        _ = entry_cb(&self.state, null, 0, 0);
                        return .disarm;
                    }
                    continue;
                }

                if (!block.magic.gnu.isValid() and !block.magic.posix.isValid()) {
                    logger.err("Invalid magic number in tar header\n", .{});
                    return .disarm;
                }
                const u_chksum = tardefs.calcChecksum(block, u8);
                const s_chksum = tardefs.calcChecksum(block, i8);
                const block_chksum = octal.octalAsciiToSize(&block.chksum);
                if (block_chksum != u_chksum and block_chksum != s_chksum) {
                    logger.err("Checksum mismatch in tar header: {} != (with u8) {} or (with i8) {}", .{ block_chksum, u_chksum, s_chksum });
                    return .disarm;
                }

                const size = octal.octalAsciiToSize(&block.size);
                const data_n_blocks = (size + tardefs.block_size - 1) / tardefs.block_size;

                if (mem.indexOf(u8, &block.name, "@PaxHeader") == null) {
                    if (!entry_cb(&self.state, block, self.offset + buf_offset, size)) {
                        return .disarm;
                    }
                }

                buf_offset += data_n_blocks * tardefs.block_size;
            }

            self.offset += buf_offset;

            self.f.pread(l, &self.completion, .{ .slice = &self.read_buf }, self.offset, Self, self, readCallback);

            return .disarm;
        }

        pub fn enqueueRead(self: *Self, loop: *xev.Loop) void {
            self.f.pread(loop, &self.completion, .{ .slice = &self.read_buf }, self.offset, Self, self, readCallback);
        }
    };
}

// Dir scanner

fn fnameCmp(_: void, a: []const u8, b: []const u8) bool {
    return mem.lessThan(u8, a, b);
}

const FileList = struct {
    allocator: mem.Allocator,
    entries: [][]const u8,

    pub fn deinit(self: *FileList) void {
        for (self.entries) |entry| {
            self.allocator.free(entry);
        }
        self.allocator.free(self.entries);
    }
};

pub fn scanDirAlloc(allocator: mem.Allocator, base_path: [:0]const u8, ext: [:0]const u8, comptime sorted: bool) !FileList {
    const logger = std.log.scoped(.scanDirAlloc);

    var entries = try std.ArrayList([]const u8).initCapacity(allocator, 1024);

    var stack = try std.ArrayListUnmanaged(std.fs.Dir).initCapacity(allocator, 8);
    defer stack.deinit(allocator);

    const real_base_path = try std.fs.realpathAlloc(allocator, base_path);
    try stack.append(allocator, try std.fs.openDirAbsolute(real_base_path, .{ .iterate = true }));
    allocator.free(real_base_path);

    while (stack.items.len > 0) {
        var dir_entry = stack.pop().?;
        var iter = dir_entry.iterate();
        while (try iter.next()) |entry| {
            var buf: [2048]u8 = undefined;
            switch (entry.kind) {
                .directory => {
                    if (dir_entry.openDir(entry.name, .{ .iterate = true })) |subdir| {
                        try stack.append(allocator, subdir);
                    } else |err| {
                        const full_path = try dir_entry.realpath(entry.name, &buf);
                        logger.warn("Failed to open subdir at {s} ({}), skipping subtree", .{ full_path, err });
                    }
                },
                .file => if (mem.endsWith(u8, entry.name, ext)) {
                    const full_path = try dir_entry.realpath(entry.name, &buf);
                    try entries.append(try allocator.dupe(u8, full_path));
                },
                .sym_link => logger.warn("Skipping symlink {s}", .{entry.name}),
                else => {},
            }
        }
        dir_entry.close();
    }

    if (sorted) {
        std.sort.block([]const u8, entries.items, {}, fnameCmp);
    }

    const out_entries = FileList{
        .allocator = allocator,
        .entries = try entries.toOwnedSlice(),
    };

    return out_entries;
}
