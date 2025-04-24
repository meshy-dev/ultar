// implicit struct: XevOStream

const std = @import("std");
const xev = @import("xev");

const logger = std.log.scoped(.XevOstream);

const Self = @This();

const buffer_size = 0x10_000;

const Chunk = struct {
    buf: [buffer_size]u8 = undefined,
    len: usize = 0,

    c: xev.Completion = undefined,

    const List = std.SinglyLinkedList(Chunk);

    pub fn remaining(self: *Chunk) []u8 {
        return self.buf[self.len..];
    }

    pub fn getWritten(self: *Chunk) []const u8 {
        return self.buf[0..self.len];
    }
};

curr_chunk: ?*Chunk.List.Node = null,
free_chunks: Chunk.List = .{},

arena: std.heap.ArenaAllocator,
loop: *xev.Loop,
file: std.fs.File,
xfile: xev.File,

pub const ReadError = error{};
pub const WriteError = error{OutOfMemory};

pub fn init(loop: *xev.Loop, output_file: std.fs.File) !Self {
    return .{
        .arena = std.heap.ArenaAllocator.init(std.heap.page_allocator),
        .loop = loop,
        .file = output_file,
        .xfile = try xev.File.init(output_file),
    };
}

pub fn deinit(self: *Self) void {
    self.curr_chunk = null;
    self.free_chunks = .{};
    self.arena.deinit();
    self.file.close();
}

pub fn read(_: *Self, _: []u8) ReadError!usize {
    unreachable;
}

pub fn getOrAllocCurrChunk(self: *Self) !*Chunk.List.Node {
    if (self.curr_chunk) |n| {
        return n;
    }

    const n = self.free_chunks.popFirst() orelse try self.arena.allocator().create(Chunk.List.Node);
    n.* = .{ .data = .{} };
    self.curr_chunk = n;
    std.log.debug("getOrAllocCurrChunk buf addr {}", .{&self.curr_chunk.?.data.buf[0]});
    return n;
}

fn writeCb(
    ud: ?*Self,
    _: *xev.Loop,
    c: *xev.Completion,
    _: xev.File,
    b: xev.WriteBuffer,
    r: xev.WriteError!usize,
) xev.CallbackAction {
    const self = ud orelse unreachable;

    // The completion is a member of Chunk
    const chunk: *Chunk = @fieldParentPtr("c", c);
    const node: *Chunk.List.Node = @fieldParentPtr("data", chunk);

    logger.debug("writeCb: {} bytes written from {}", .{ r catch 0, &b.slice[0] });

    self.free_chunks.prepend(node);

    return .disarm;
}

pub fn write(self: *Self, bytes: []const u8) WriteError!usize {
    if (bytes.len == 0) return 0;

    var rem = bytes;

    while (rem.len > 0) {
        const chunk_node = try self.getOrAllocCurrChunk();

        // Try to write to the last chunk
        const chunk = &chunk_node.data;
        const remaining_buf = chunk.remaining();
        const n = @min(remaining_buf.len, rem.len);
        @memcpy(remaining_buf[0..n], rem[0..n]);
        chunk.len += n;
        rem = rem[n..];

        // If chunk is full, submit to write
        if (chunk.remaining().len == 0) {
            self.flush();
        }
    }

    return bytes.len;
}

pub fn flush(self: *Self) void {
    if (self.curr_chunk) |chunk_node| {
        var chunk = &chunk_node.data;
        const slice = chunk.getWritten();
        if (slice.len == 0) return;

        self.xfile.write(self.loop, &chunk.c, .{ .slice = slice }, Self, self, writeCb);
        self.curr_chunk = null;
    }
}
