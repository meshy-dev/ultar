// implicit struct: XevOStream

const std = @import("std");
const xev = @import("xev");

const logger = std.log.scoped(.XevOstream);

const Self = @This();

const buffer_size = 0x10_000;

const List = std.SinglyLinkedList;

const Chunk = struct {
    node: List.Node = .{},
    buf: [buffer_size]u8 = undefined,

    c: xev.Completion = undefined,
};

curr_chunk: ?*Chunk = null,
free_chunks: List = .{},

arena: std.heap.ArenaAllocator,
loop: *xev.Loop,
file: std.fs.File,
xfile: xev.File,
interface: std.Io.Writer,

pub fn init(loop: *xev.Loop, output_file: std.fs.File) !Self {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    const first_chunk = try arena.allocator().create(Chunk);
    return .{
        .curr_chunk = first_chunk,
        .arena = arena,
        .loop = loop,
        .file = output_file,
        .xfile = try xev.File.init(output_file),
        .interface = std.Io.Writer{
            .vtable = &.{ .drain = Self.drain },
            .buffer = &first_chunk.buf,
        },
    };
}

pub fn deinit(self: *Self) void {
    self.curr_chunk = null;
    self.free_chunks = .{};
    self.arena.deinit();
    self.file.close();
}

pub fn getOrAllocChunk(self: *Self) !*Chunk {
    const n = self.free_chunks.popFirst() orelse &(try self.arena.allocator().create(Chunk)).node;
    const chunk: *Chunk = @fieldParentPtr("node", n);
    chunk.* = .{};
    self.curr_chunk = chunk;
    std.log.debug("getOrAllocChunk buf addr {}", .{&self.curr_chunk.?.buf[0]});
    return chunk;
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

    logger.debug("writeCb: {} bytes written from {}", .{ r catch 0, &b.slice[0] });

    self.free_chunks.prepend(&chunk.node);

    return .disarm;
}

// fn write_impl(self: *Self, bytes: []const u8) std.io.Writer.Error!void {
//
//     // const chunk = self.getOrAllocCurrChunk() catch return error.WriteFailed;
//
//     // Try to write to the last chunk
//     const remaining_buf = chunk.remaining();
//     const n = @min(remaining_buf.len, bytes.len);
//     @memcpy(remaining_buf[0..n], bytes[0..n]);
//     chunk.len += n;
//     const rem = bytes[n..];
//
//     // If chunk is full, submit to write
//     if (chunk.remaining().len == 0) {
//         self.flush();
//     }
//
//     if (rem.len > 0) {
//         return self.write_impl(rem);
//     }
// }

fn drain(io_w: *std.Io.Writer, data: []const []const u8, splat: usize) std.io.Writer.Error!usize {
    _ = splat;
    _ = data;
    var self: *Self = @fieldParentPtr("interface", io_w);

    // buffer aliases to curr_chunk.buf
    // Send the current chunk if we have data
    if (io_w.end > 0) {
        self.flush() catch return error.WriteFailed;
    }

    return 0;
}

pub fn flush(self: *Self) !void {
    if (self.curr_chunk) |chunk| {
        const slice = chunk.buf[0..self.interface.end];
        self.xfile.write(self.loop, &chunk.c, .{ .slice = slice }, Self, self, writeCb);

        self.curr_chunk = try self.getOrAllocChunk();
        self.interface.buffer = &self.curr_chunk.?.buf;
        self.interface.end = 0;
    }
}
