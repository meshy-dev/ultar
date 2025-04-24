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
            buf[0] = 0xda;
            packBE(u16, buf[1..], @intCast(len));
            return buf[0..3];
        },
        .array32 => {
            buf[0] = 0xdb;
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

pub fn Packer(comptime Writer: type) type {
    // Ensure writer has write method
    comptime {
        std.debug.assert(std.meta.hasMethod(Writer, "write"));
    }

    return struct {
        const Self = @This();

        writer: *Writer,

        pub fn init(writer: *Writer) Self {
            return .{
                .writer = writer,
            };
        }

        pub fn addNil(self: *Self) !void {
            _ = try self.writer.write(&[_]u8{m_nil});
        }

        pub fn addBool(self: *Self, v: bool) !void {
            _ = try self.writer.write(if (v) &[_]u8{m_true} else &[_]u8{m_false});
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
}

test "test writer" {
    var buf: [1024]u8 = std.mem.zeroes([1024]u8);
    var writer = std.io.FixedBufferStream([]u8){ .buffer = &buf, .pos = 0 };

    var pack = Packer(@TypeOf(writer)).init(&writer);
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

    const ref = "\x84\xa3abc\xc2\xa3def7\xb4%%%%%%%%%%%%%%%%%%%%\xce\xba\xad\xf0\r\xb1this is an \xe5\x90\x91\xe9\x87\x8f\x93\xcb\xbf\xec\xcc\xcc\xcc\xcc\xcc\xcd\xcb?\xe0Q\xeb\x85\x1e\xb8R\xcb?\xd5UUUUUU";
    const gen = writer.getWritten();

    try std.testing.expectEqualSlices(u8, ref, gen);
}
