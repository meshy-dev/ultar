const std = @import("std");

fn truncateToAny(comptime Target: type, n: anytype) Target {
    const T = @typeInfo(@TypeOf(n)).int;
    const target_sign = @typeInfo(Target).int.signedness;
    if (T.signedness == target_sign) return @truncate(n);

    const U = std.meta.Int(target_sign, T.bits);
    const u: U = @bitCast(n);
    return @truncate(u);
}

fn packU8(comptime T: type, op_num: comptime_int, n: anytype) u8 {
    const n_bits = 8 - @bitSizeOf(T);
    const out: u8 = @as(u8, op_num << n_bits) | truncateToAny(u8, n);
    return out;
}

// Endieness
fn packBE(comptime T: type, buf: []u8, v: T) void {
    comptime {
        std.debug.assert(@typeInfo(T) == .int);
    }
    const nb = @bitSizeOf(T) / 8;
    const UType = @Type(std.builtin.Type{ .int = .{
        .signedness = .unsigned,
        .bits = @bitSizeOf(T),
    } });
    const uv: UType = @bitCast(v);
    inline for (0..nb) |i| {
        const shift: comptime_int = (nb - 1 - i) * 8;
        buf[i] = @truncate(uv >> shift);
    }
}

test "pack BE" {
    var buf: [9]u8 = [_]u8{0} ** 9;

    packBE(u16, &buf, 0x1234);
    try std.testing.expectEqual(buf[0], 0x12);
    try std.testing.expectEqual(buf[1], 0x34);

    packBE(u32, &buf, 0xabcd5678);
    try std.testing.expectEqual(buf[0], 0xab);
    try std.testing.expectEqual(buf[1], 0xcd);
    try std.testing.expectEqual(buf[2], 0x56);
    try std.testing.expectEqual(buf[3], 0x78);

    packBE(u64, &buf, 0x1234abcd9abcdef0);
    try std.testing.expectEqual(buf[0], 0x12);
    try std.testing.expectEqual(buf[1], 0x34);
    try std.testing.expectEqual(buf[2], 0xab);
    try std.testing.expectEqual(buf[3], 0xcd);
    try std.testing.expectEqual(buf[4], 0x9a);
    try std.testing.expectEqual(buf[5], 0xbc);
    try std.testing.expectEqual(buf[6], 0xde);
    try std.testing.expectEqual(buf[7], 0xf0);

    packBE(i16, &buf, -0x1234);
    try std.testing.expectEqual(buf[0], 0xed);
    try std.testing.expectEqual(buf[1], 0xcc);
}

// https://github.com/msgpack/msgpack/blob/master/spec.md#nil-format
pub const m_nil: u8 = 0xc0;

// https://github.com/msgpack/msgpack/blob/master/spec.md#bool-format-family
pub const m_false: u8 = 0xc2;
pub const m_true: u8 = 0xc3;

// https://github.com/msgpack/msgpack/blob/master/spec.md#int-format-family

const IntFamily = enum {
    pos_fixint,
    neg_fixint,
    int8,
    int16,
    int32,
    int64,
    uint8,
    uint16,
    uint32,
    uint64,

    pub fn determine(comptime T: type, v: T) IntFamily {
        comptime {
            std.debug.assert(@typeInfo(T) == .int);
        }
        if (v >= 0) {
            if (v <= std.math.maxInt(u7)) {
                return .pos_fixint;
            } else if (v <= std.math.maxInt(u8)) {
                return .uint8;
            } else if (v <= std.math.maxInt(u16)) {
                return .uint16;
            } else if (v <= std.math.maxInt(u32)) {
                return .uint32;
            } else {
                return .uint64;
            }
        } else {
            if (v >= std.math.minInt(i6)) {
                // 5 bits but the prefix are all 1
                return .neg_fixint;
            } else if (v >= std.math.minInt(i8)) {
                return .int8;
            } else if (v >= std.math.minInt(i16)) {
                return .int16;
            } else if (v >= std.math.minInt(i32)) {
                return .int32;
            } else {
                return .int64;
            }
        }
    }
};

pub fn packInt(comptime T: type, v: T, buf: []u8) []u8 {
    const fam = IntFamily.determine(T, v);
    switch (fam) {
        .pos_fixint => {
            buf[0] = packU8(u1, 0, v);
            return buf[0..1];
        },
        .neg_fixint => {
            buf[0] = packU8(u3, 0b111, v);
            return buf[0..1];
        },
        .uint8 => {
            buf[0] = 0xcc;
            buf[1] = truncateToAny(u8, v);
            return buf[0..2];
        },
        .uint16 => {
            buf[0] = 0xcd;
            packBE(u16, buf[1..], @intCast(v));
            return buf[0..3];
        },
        .uint32 => {
            buf[0] = 0xce;
            packBE(u32, buf[1..], @intCast(v));
            return buf[0..5];
        },
        .uint64 => {
            buf[0] = 0xcf;
            packBE(u64, buf[1..], @intCast(v));
            return buf[0..9];
        },
        .int8 => {
            buf[0] = 0xd0;
            buf[1] = truncateToAny(u8, v);
            return buf[0..2];
        },
        .int16 => {
            buf[0] = 0xd1;
            packBE(i16, buf[1..], @intCast(v));
            return buf[0..3];
        },
        .int32 => {
            buf[0] = 0xd2;
            packBE(i32, buf[1..], @intCast(v));
            return buf[0..5];
        },
        .int64 => {
            buf[0] = 0xd3;
            packBE(i64, buf[1..], @intCast(v));
            return buf[0..9];
        },
    }
}

test "test int packing" {
    {
        var buf: [9]u8 = @splat(42);
        const out = packInt(u32, 5, &buf);
        try std.testing.expectEqualSlices(u8, &[_]u8{5}, out);
        try std.testing.expectEqualSlices(u8, &([_]u8{5} ++ .{42} ** 8), &buf);
    }
    {
        var buf: [9]u8 = @splat(42);
        const out = packInt(i8, -5, &buf);
        try std.testing.expectEqualSlices(u8, &[_]u8{0xfb}, out);
        try std.testing.expectEqualSlices(u8, &([_]u8{0xfb} ++ .{42} ** 8), &buf);
    }
    {
        var buf: [9]u8 = @splat(42);
        const out = packInt(i10, 128, &buf);
        try std.testing.expectEqualSlices(u8, &[_]u8{ 0xcc, 0x80 }, out);
        try std.testing.expectEqualSlices(u8, &([_]u8{ 0xcc, 0x80 } ++ .{42} ** 7), &buf);
    }
    {
        var buf: [9]u8 = @splat(42);
        const out = packInt(u32, 0xbaadf00d, &buf);
        try std.testing.expectEqualSlices(u8, &[_]u8{ 0xce, 0xba, 0xad, 0xf0, 0x0d }, out);
        try std.testing.expectEqualSlices(u8, &([_]u8{ 0xce, 0xba, 0xad, 0xf0, 0x0d } ++ .{42} ** 4), &buf);
    }
    {
        var buf: [9]u8 = @splat(42);
        const out = packInt(i64, 0x420baadf00d, &buf);
        try std.testing.expectEqualSlices(u8, &[_]u8{ 0xcf, 0, 0, 0x04, 0x20, 0xba, 0xad, 0xf0, 0x0d }, out);
        try std.testing.expectEqualSlices(u8, &[_]u8{ 0xcf, 0, 0, 0x04, 0x20, 0xba, 0xad, 0xf0, 0x0d }, &buf);
    }
    {
        var buf: [9]u8 = @splat(42);
        const out = packInt(i64, -555, &buf);
        try std.testing.expectEqualSlices(u8, &[_]u8{ 0xd1, 0xfd, 0xd5 }, out);
    }
}

// https://github.com/msgpack/msgpack/blob/master/spec.md#float-format-family

pub const FloatFamily = enum {
    float32,
    float64,

    pub fn determine(comptime T: type, v: T) FloatFamily {
        comptime {
            std.debug.assert(@typeInfo(T) == .float);
        }
        const f_info = @typeInfo(T).float;
        if (f_info.bits <= 32) {
            return .float32;
        } else {
            const v32: f32 = @floatCast(v);
            const v32_in_64: f64 = @floatCast(v32);
            if (v32_in_64 == v) {
                return .float32;
            }
            return .float64;
        }
    }
};

pub fn packFloat(comptime T: type, v: T, buf: []u8) []u8 {
    const fam = FloatFamily.determine(T, v);
    switch (fam) {
        .float32 => {
            buf[0] = 0xca;
            const vf: f32 = @floatCast(v);
            const vu: u32 = @bitCast(vf);
            packBE(u32, buf[1..], vu);
            return buf[0..5];
        },
        .float64 => {
            buf[0] = 0xcb;
            const vf: f64 = @floatCast(v);
            const vu: u64 = @bitCast(vf);
            packBE(u64, buf[1..], vu);
            return buf[0..9];
        },
    }
}

// https://github.com/msgpack/msgpack/blob/master/spec.md#str-format-family

pub const PackError = error{TooManyElements};

const StrFamily = enum {
    fixstr,
    str8,
    str16,
    str32,

    pub fn determine(l: usize) PackError!StrFamily {
        if (l <= std.math.maxInt(u5)) {
            return .fixstr;
        } else if (l <= std.math.maxInt(u8)) {
            return .str8;
        } else if (l <= std.math.maxInt(u16)) {
            return .str16;
        } else if (l <= std.math.maxInt(u32)) {
            return .str32;
        } else {
            return PackError.TooManyElements;
        }
    }
};

pub fn packStrHeader(len: usize, buf: []u8) PackError![]u8 {
    const fam = try StrFamily.determine(len);
    switch (fam) {
        .fixstr => {
            buf[0] = packU8(u3, 0b101, len);
            return buf[0..1];
        },
        .str8 => {
            buf[0] = 0xd9;
            buf[1] = truncateToAny(u8, len);
            return buf[0..2];
        },
        .str16 => {
            buf[0] = 0xda;
            packBE(u16, buf[1..], @intCast(len));
            return buf[0..3];
        },
        .str32 => {
            buf[0] = 0xdb;
            packBE(u32, buf[1..], @intCast(len));
            return buf[0..5];
        },
    }
}

// https://github.com/msgpack/msgpack/blob/master/spec.md#bin-format-family
// TODO: Not implemented yet

// https://github.com/msgpack/msgpack/blob/master/spec.md#array-format-family

const ArrayFamily = enum {
    fixarray,
    array16,
    array32,

    pub fn determine(l: usize) PackError!ArrayFamily {
        if (l <= std.math.maxInt(u4)) {
            return .fixarray;
        } else if (l <= std.math.maxInt(u16)) {
            return .array16;
        } else if (l <= std.math.maxInt(u32)) {
            return .array32;
        } else {
            return PackError.TooManyElements;
        }
    }
};

pub fn packArrayHeader(len: usize, buf: []u8) PackError![]u8 {
    const fam = try ArrayFamily.determine(len);
    switch (fam) {
        .fixarray => {
            buf[0] = packU8(u4, 0b1001, len);
            return buf[0..1];
        },
        .array16 => {
            buf[0] = 0xdc;
            packBE(u16, buf[1..], @intCast(len));
            return buf[0..3];
        },
        .array32 => {
            buf[0] = 0xdd;
            packBE(u32, buf[1..], @intCast(len));
            return buf[0..5];
        },
    }
}

// https://github.com/msgpack/msgpack/blob/master/spec.md#map-format-family

const MapFamily = enum {
    fixmap,
    map16,
    map32,

    pub fn determine(l: usize) PackError!MapFamily {
        if (l <= std.math.maxInt(u4)) {
            return .fixmap;
        } else if (l <= std.math.maxInt(u16)) {
            return .map16;
        } else if (l <= std.math.maxInt(u32)) {
            return .map32;
        } else {
            return PackError.TooManyElements;
        }
    }
};

pub fn packMapHeader(len: usize, buf: []u8) PackError![]u8 {
    const fam = try MapFamily.determine(len);
    switch (fam) {
        .fixmap => {
            buf[0] = packU8(u4, 0b1000, len);
            return buf[0..1];
        },
        .map16 => {
            buf[0] = 0xde;
            packBE(u16, buf[1..], @intCast(len));
            return buf[0..3];
        },
        .map32 => {
            buf[0] = 0xdf;
            packBE(u32, buf[1..], @intCast(len));
            return buf[0..5];
        },
    }
}

test "test map packing" {
    {
        var buf: [1]u8 = undefined;
        const out = try packMapHeader(0, &buf);
        try std.testing.expectEqualSlices(u8, &[_]u8{0x80}, out);
        try std.testing.expectEqualSlices(u8, &[_]u8{0x80}, &buf);
    }
}

// https://github.com/msgpack/msgpack/blob/master/spec.md#ext-format-family
// TODO: Not implemented yet

// https://github.com/msgpack/msgpack/blob/master/spec.md#timestamp-extension-type
// NOTE: Will not implement this for now

// IO based serialization

pub const Packer = struct {
    const Self = @This();

    writer: *std.io.Writer,

    pub fn addNil(self: *Self) !void {
        _ = try self.writer.writeByte(m_nil);
    }

    pub fn addBool(self: *Self, v: bool) !void {
        _ = try self.writer.writeByte(if (v) m_true else m_false);
    }

    pub fn addInt(self: *Self, comptime T: type, v: T) !void {
        var buf: [9]u8 = undefined;
        const bytes = packInt(T, v, buf[0..]);
        _ = try self.writer.write(bytes);
    }

    pub fn addFloat(self: *Self, comptime T: type, v: T) !void {
        var buf: [9]u8 = undefined;
        const bytes = packFloat(T, v, buf[0..]);
        _ = try self.writer.write(bytes);
    }

    pub fn addStr(self: *Self, str: []const u8) !void {
        var buf: [5]u8 = undefined;
        const header_bytes = try packStrHeader(str.len, buf[0..]);
        _ = try self.writer.write(header_bytes);
        _ = try self.writer.write(str);
    }

    pub fn beginArray(self: *Self, len: usize) !void {
        var buf: [5]u8 = undefined;
        const header_bytes = try packArrayHeader(len, buf[0..]);
        _ = try self.writer.write(header_bytes);
    }

    pub fn beginMap(self: *Self, len: usize) !void {
        var buf: [5]u8 = undefined;
        const header_bytes = try packMapHeader(len, buf[0..]);
        _ = try self.writer.write(header_bytes);
    }
};

test "test writer" {
    var buf: [1024]u8 = std.mem.zeroes([1024]u8);

    {
        var writer = std.io.Writer.fixed(&buf);

        var pack = Packer{ .writer = &writer };
        try pack.beginMap(4);
        const k0: *const [3:0]u8 = "abc";
        try pack.addStr(k0);
        try pack.addBool(false);
        const k1: *const [3]u8 = "def";
        try pack.addStr(k1);
        try pack.addInt(u32, 55);
        const k2: [20]u8 = @splat('%');
        try pack.addStr(&k2);
        try pack.addInt(u64, 0xbaadf00d);
        const k3 = "this is an 向量";
        try pack.addStr(k3);
        try pack.beginArray(3);
        try pack.addFloat(f64, -0.9);
        try pack.addFloat(f64, 0.51);
        try pack.addFloat(f64, 1.0 / 3.0);
        try writer.flush();

        const ref = "\x84\xa3abc\xc2\xa3def7\xb4%%%%%%%%%%%%%%%%%%%%\xce\xba\xad\xf0\r\xb1this is an \xe5\x90\x91\xe9\x87\x8f\x93\xcb\xbf\xec\xcc\xcc\xcc\xcc\xcc\xcd\xcb?\xe0Q\xeb\x85\x1e\xb8R\xcb?\xd5UUUUUU";
        const gen = buf[0..writer.end];

        try std.testing.expectEqualSlices(u8, ref, gen);
    }

    {
        var writer = std.io.Writer.fixed(&buf);

        var pack = Packer{ .writer = &writer };

        try pack.beginArray(100);
        for (0..100) |i| try pack.addInt(usize, i);
        try writer.flush();

        const ref = "\xdc\x00d\x00\x01\x02\x03\x04\x05\x06\x07\x08\t\n\x0b\x0c\r\x0e\x0f\x10\x11\x12\x13\x14\x15\x16\x17\x18\x19\x1a\x1b\x1c\x1d\x1e\x1f !\"#$%&\'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`abc";
        const gen = buf[0..writer.end];

        try std.testing.expectEqualSlices(u8, ref, gen);
    }
}

// Unpacking

pub fn MsgpackHandler(CtxT: type) type {
    return struct {
        const T = CtxT;

        nil: ?fn (ud: *CtxT) anyerror!void = null,
        bool: ?fn (ud: *CtxT, v: bool) anyerror!void = null,
        uint: ?fn (ud: *CtxT, v: u64) anyerror!void = null,
        int: ?fn (ud: *CtxT, v: i64) anyerror!void = null,
        float: ?fn (ud: *CtxT, v: f64) anyerror!void = null,
        str: ?fn (ud: *CtxT, str: []const u8) anyerror!void = null,
        mapBegin: ?fn (ud: *CtxT, len: usize) anyerror!void = null,
        mapField: ?fn (ud: *CtxT, key: []const u8) anyerror!void = null,
        mapEnd: ?fn (ud: *CtxT) anyerror!void = null,
        arrayBegin: ?fn (ud: *CtxT, len: usize) anyerror!void = null,
        arrayEnd: ?fn (ud: *CtxT) anyerror!void = null,
    };
}

const up_log = std.log.scoped(.msgpack_unpacker);

fn errNoImpl() error{NotImplemented} {
    std.debug.dumpCurrentStackTrace(null);
    up_log.err("Not implemented", .{});
    return error.NotImplemented;
}

const MsgpackError = error{
    InvalidHeader,
    MapKeyIsNotStr,
};

pub fn Unpacker(CtxT: type, comptime handler: MsgpackHandler(CtxT)) type {
    return struct {
        const Self = @This();

        ctx: CtxT,
        reader: *std.io.Reader,
        alloc: std.mem.Allocator,

        fn isShortStr(header: u8) bool {
            return header == 0xd9 or (header >= 0xa0 and header <= 0xbf);
        }

        fn unpackStr(self: *Self, header: u8, out_buf: []u8) !struct { []u8, u64 } {
            var read: usize = 0;
            var str_len: usize = 0;
            switch (header) {
                // fixstr
                0xa0...0xbf => {
                    str_len = @intCast(header & 0x1f);
                },
                // str 8
                0xd9 => {
                    var buf: [1]u8 = .{0};
                    read += try self.reader.readSliceShort(&buf);
                    str_len = @intCast(buf[0]);
                },
                // str 16
                0xda => {
                    var buf: [2]u8 = undefined;
                    read += try self.reader.readSliceShort(&buf);
                    str_len = (@as(u64, @intCast(buf[0])) << 8) | @as(u64, @intCast(buf[1]));
                },
                // str 32
                0xdb => {
                    var buf: [4]u8 = undefined;
                    read += try self.reader.readSliceShort(&buf);
                    str_len = (@as(u64, @intCast(buf[0])) << 24) | (@as(u64, @intCast(buf[1])) << 16) | (@as(u64, @intCast(buf[2])) << 8) | @as(u64, @intCast(buf[3]));
                },
                else => return error.InvalidHeader,
            }
            if (str_len > out_buf.len) {
                return error.OutOfMemory;
            }
            try self.reader.readSliceAll(out_buf[0..str_len]);
            read += str_len;
            return .{ out_buf[0..str_len], read };
        }

        fn unpackStrAlloc(self: *Self, header: u8) !struct { []u8, u64 } {
            var read: usize = 0;
            var str_len: usize = 0;
            switch (header) {
                // fixstr
                0xa0...0xbf => {
                    str_len = @intCast(header & 0x1f);
                },
                // str 8
                0xd9 => {
                    var buf: [1]u8 = .{0};
                    read += try self.reader.readSliceShort(&buf);
                    str_len = @intCast(buf[0]);
                },
                // str 16
                0xda => {
                    var buf: [2]u8 = undefined;
                    read += try self.reader.readSliceShort(&buf);
                    str_len = (@as(u64, @intCast(buf[0])) << 8) | @as(u64, @intCast(buf[1]));
                },
                // str 32
                0xdb => {
                    var buf: [4]u8 = undefined;
                    read += try self.reader.readSliceShort(&buf);
                    str_len = (@as(u64, @intCast(buf[0])) << 24) | (@as(u64, @intCast(buf[1])) << 16) | (@as(u64, @intCast(buf[2])) << 8) | @as(u64, @intCast(buf[3]));
                },
                else => return error.InvalidHeader,
            }
            const str = try self.alloc.alloc(u8, str_len);
            try self.reader.readSliceAll(str);
            read += str_len;
            return .{ str, read };
        }

        fn walkMap(self: *Self, header: u8) !u64 {
            var read: u64 = 0;
            var map_len: usize = 0;
            switch (header) {
                // fixmap
                0x80...0x8f => map_len = @intCast(header & 0x0f),
                // map 16
                0xde => {
                    var buf: [2]u8 = undefined;
                    read += try self.reader.readSliceShort(&buf);
                    map_len = (@as(u64, @intCast(buf[0])) << 8) | @as(u64, @intCast(buf[1]));
                },
                // map 32
                0xdf => {
                    var buf: [4]u8 = undefined;
                    read += try self.reader.readSliceShort(&buf);
                    map_len = (@as(u64, @intCast(buf[0])) << 24) | (@as(u64, @intCast(buf[1])) << 16) | (@as(u64, @intCast(buf[2])) << 8) | @as(u64, @intCast(buf[3]));
                },
                else => unreachable,
            }

            try handler.mapBegin.?(&self.ctx, map_len);

            for (0..map_len) |idx| {
                // Read key
                var key_header: u8 = 0;
                read += try self.reader.readSliceShort(@ptrCast(&key_header));
                if (Self.isShortStr(key_header)) {
                    var buf: [256]u8 = undefined;
                    const key, const key_n = self.unpackStr(key_header, &buf) catch |err| {
                        if (err == MsgpackError.InvalidHeader) {
                            std.log.err("Map entry #{}, key header {x} is not str.", .{ idx, key_header });
                            return MsgpackError.MapKeyIsNotStr;
                        }
                        return err;
                    };
                    read += key_n;
                    try handler.mapField.?(&self.ctx, key);
                } else {
                    const key, const key_n = self.unpackStrAlloc(key_header) catch |err| {
                        if (err == MsgpackError.InvalidHeader) {
                            std.log.err("Map entry #{}, key header {x} is not str.", .{ idx, key_header });
                            return MsgpackError.MapKeyIsNotStr;
                        }
                        return err;
                    };
                    defer self.alloc.free(key);
                    read += key_n;
                    try handler.mapField.?(&self.ctx, key);
                }

                // Process value
                read += try self.next(1);
            }

            if (handler.mapEnd) |f| {
                try f(&self.ctx);
            }

            return read;
        }

        fn walkArr(self: *Self, header: u8) !u64 {
            var read: u64 = 0;
            var arr_len: usize = 0;
            switch (header) {
                // fixarray
                0x90...0x9f => arr_len = @intCast(header & 0x0f),
                // array 16
                0xdc => {
                    var buf: [2]u8 = undefined;
                    read += try self.reader.readSliceShort(&buf);
                    arr_len = (@as(u64, @intCast(buf[0])) << 8) | @as(u64, @intCast(buf[1]));
                },
                // array 32
                0xdd => {
                    var buf: [4]u8 = undefined;
                    read += try self.reader.readSliceShort(&buf);
                    arr_len = (@as(u64, @intCast(buf[0])) << 24) | (@as(u64, @intCast(buf[1])) << 16) | (@as(u64, @intCast(buf[2])) << 8) | @as(u64, @intCast(buf[3]));
                },
                else => unreachable,
            }

            try (handler.arrayBegin orelse return errNoImpl())(&self.ctx, arr_len);

            for (0..arr_len) |_| {
                read += try self.next(1);
            }

            if (handler.arrayEnd) |f| {
                try f(&self.ctx);
            }

            return read;
        }

        // fn unpackMapContent(self: *Self, n: usize) !u64 {
        //     for (0..n) |_| {
        //         var str_header: u8 = 0;
        //     }
        // }

        pub fn next(self: *Self, limit_n: usize) anyerror!u64 {
            var read: u64 = 0;

            for (0..limit_n) |_| {
                var header: u8 = 0;
                if (try self.reader.readSliceShort(@ptrCast(&header)) == 0) {
                    return error.EndOfStream;
                }
                read += 1;

                try switch (header) {
                    // positive fixint
                    0x00...0x7f => if (handler.uint) |h_uint| {
                        _ = try h_uint(&self.ctx, @intCast(header));
                    } else return errNoImpl(),
                    // fixmap, map 16, map 32
                    0x80...0x8f, 0xde, 0xdf => {
                        if (handler.mapBegin == null or handler.mapField == null) return errNoImpl();
                        read += try self.walkMap(header);
                    },
                    // fixarray, arr 16, arr 32
                    0x90...0x9f, 0xdc, 0xdd => read += try self.walkArr(header),
                    // fixstr, str 8
                    0xa0...0xbf, 0xd9 => if (handler.str) |h_str| {
                        var buf: [256]u8 = undefined;
                        const str, const n = try self.unpackStr(header, &buf);
                        _ = try h_str(&self.ctx, str);
                        read += n;
                    } else return errNoImpl(),
                    // str 16, str 32
                    0xda, 0xdb => if (handler.str) |h_str| {
                        const str, const n = try self.unpackStrAlloc(header);
                        defer self.alloc.free(str);
                        _ = try h_str(&self.ctx, str);
                        read += n;
                    } else return errNoImpl(),
                    // nil
                    0xc0 => (handler.nil orelse return errNoImpl())(&self.ctx),
                    // (never used)
                    0xc1 => return error.InvalidHeader,
                    // false
                    0xc2 => (handler.bool orelse return errNoImpl())(&self.ctx, false),
                    // true
                    0xc3 => (handler.bool orelse return errNoImpl())(&self.ctx, true),
                    // bin 8, bin 16, bin 32
                    0xc4, 0xc5, 0xc6 => return errNoImpl(),
                    // ext 8, ext 16, ext 32
                    0xc7, 0xc8, 0xc9 => return errNoImpl(),
                    // float 32
                    0xca => if (handler.float) |h_float| {
                        var buf: [4]u8 = undefined;
                        read += try self.reader.readSliceShort(&buf);
                        const u = (@as(u32, @intCast(buf[0])) << 24) | (@as(u32, @intCast(buf[1])) << 16) | (@as(u32, @intCast(buf[2])) << 8) | @as(u32, @intCast(buf[3]));
                        try h_float(&self.ctx, @floatCast(@as(f32, @bitCast(u))));
                    } else return errNoImpl(),
                    // float 64
                    0xcb => if (handler.float) |h_float| {
                        var buf: [8]u8 = undefined;
                        read += try self.reader.readSliceShort(&buf);
                        var u: u64 = 0;
                        inline for (0..8) |i| {
                            u |= (@as(u64, @intCast(buf[i])) << (56 - i * 8));
                        }
                        try h_float(&self.ctx, @floatCast(@as(f64, @bitCast(u))));
                    } else return errNoImpl(),
                    // uint 8, 16, 32, 64
                    inline 0xcc...0xcf => |h| if (handler.uint) |h_uint| {
                        const nb = 1 << (h - 0xcc);
                        var buf: [8]u8 = undefined;
                        read += try self.reader.readSliceShort(buf[0..nb]);

                        const UType = @Type(std.builtin.Type{ .int = .{ .bits = nb * 8, .signedness = .unsigned } });
                        var u: UType = 0;
                        inline for (0..nb) |i| {
                            u |= (@as(UType, @intCast(buf[i])) << ((nb - 1 - i) * 8));
                        }
                        try h_uint(&self.ctx, @intCast(u));
                    } else return errNoImpl(),
                    // int 8, 16, 32, 64
                    inline 0xd0...0xd3 => |h| if (handler.int) |h_int| {
                        const nb = 1 << (h - 0xd0);
                        var buf: [8]u8 = undefined;
                        read += try self.reader.readSliceShort(buf[0..nb]);

                        const UType = @Type(std.builtin.Type{ .int = .{ .bits = nb * 8, .signedness = .unsigned } });
                        var u: UType = 0;
                        inline for (0..nb) |i| {
                            u |= (@as(UType, @intCast(buf[i])) << ((nb - 1 - i) * 8));
                        }
                        try h_int(&self.ctx, @bitCast(@as(u64, @intCast(u))));
                    } else return errNoImpl(),
                    // fixext 1, 2, 4, 8, 16
                    0xd4...0xd8 => return errNoImpl(),
                    // negative fixint
                    0xe0...0xff => {},
                };
            }

            return read;
        }

        pub fn init(reader: std.io.Reader, ctx: CtxT, alloc: std.mem.Allocator) Self {
            return .{
                .reader = reader,
                .ctx = ctx,
                .alloc = alloc,
            };
        }
    };
}

test "test unpacker" {
    const ref = "\x84\xa3abc\xc2\xa3def7\xb4%%%%%%%%%%%%%%%%%%%%\xce\xba\xad\xf0\r\xb1this is an \xe5\x90\x91\xe9\x87\x8f\x93\xcb\xbf\xec\xcc\xcc\xcc\xcc\xcc\xcd\xcb?\xe0Q\xeb\x85\x1e\xb8R\xcb?\xd5UUUUUU";
    var reader = std.io.Reader.fixed(ref);

    var gpa = std.heap.DebugAllocator(.{}).init;
    const alloc = gpa.allocator();

    const FmtCtx = struct {
        const Self = @This();

        writer: *std.io.Writer,

        pub fn f_bool(s: *Self, v: bool) !void {
            if (v) {
                _ = try s.writer.writeAll("true, ");
            } else {
                _ = try s.writer.writeAll("false, ");
            }
        }

        pub fn fmtInt(s: *Self, v: i64) !void {
            try s.writer.print("{}, ", .{v});
        }

        pub fn fmtUint(s: *Self, v: u64) !void {
            try s.writer.print("{}, ", .{v});
        }

        pub fn fmtFloat(s: *Self, v: f64) !void {
            try s.writer.print("{}, ", .{v});
        }

        pub fn mapBegin(s: *Self, _: usize) !void {
            try s.writer.writeByte('{');
        }

        pub fn mapField(s: *Self, key: []const u8) !void {
            try s.writer.print("\"{s}\" = ", .{key});
        }

        pub fn mapEnd(s: *Self) !void {
            try s.writer.writeAll("}, ");
        }

        pub fn arrayBegin(s: *Self, _: usize) !void {
            try s.writer.writeByte('{');
        }

        pub fn arrayEnd(s: *Self) !void {
            try s.writer.writeAll("}, ");
        }
    };

    var out_buf: [1024]u8 = std.mem.zeroes([1024]u8);
    var writer = std.io.Writer.fixed(&out_buf);

    var unpacker: Unpacker(
        FmtCtx,
        .{
            .bool = FmtCtx.f_bool,
            .int = FmtCtx.fmtInt,
            .uint = FmtCtx.fmtUint,
            .float = FmtCtx.fmtFloat,
            .mapBegin = FmtCtx.mapBegin,
            .mapField = FmtCtx.mapField,
            .arrayBegin = FmtCtx.arrayBegin,
            .arrayEnd = FmtCtx.arrayEnd,
            .mapEnd = FmtCtx.mapEnd,
        },
    ) = .{ .reader = &reader, .ctx = .{ .writer = &writer }, .alloc = alloc };
    const u = try unpacker.next(1);
    try std.testing.expectEqual(ref.len, u);

    const should_unpack =
        "{\"abc\" = false, \"def\" = 55, \"%%%%%%%%%%%%%%%%%%%%\" = 3131961357, \"this is an 向量\" = {-0.9, 0.51, 0.3333333333333333, }, }, ";
    try writer.flush();
    try std.testing.expectEqualStrings(should_unpack, out_buf[0..writer.end]);

    try std.testing.expect(gpa.deinit() == .ok);
}
