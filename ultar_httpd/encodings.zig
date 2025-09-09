const std = @import("std");

pub fn urlEncodeAlloc(alloc: std.mem.Allocator, input: []const u8) ![]u8 {
    var out = try std.ArrayList(u8).initCapacity(alloc, input.len * 3);
    errdefer out.deinit(alloc);
    for (input) |b| {
        const is_unreserved = (b >= 'A' and b <= 'Z') or (b >= 'a' and b <= 'z') or (b >= '0' and b <= '9') or b == '-' or b == '_' or b == '.' or b == '~';
        if (is_unreserved) {
            try out.append(alloc, b);
        } else {
            try out.append(alloc, '%');
            const hex = "0123456789ABCDEF";
            try out.append(alloc, hex[b >> 4]);
            try out.append(alloc, hex[b & 0x0F]);
        }
    }
    return out.toOwnedSlice(alloc);
}

fn parseHexDigit(ch: u8) !u8 {
    return switch (ch) {
        '0'...'9' => ch - '0',
        'a'...'f' => ch - 'a' + 10,
        'A'...'F' => ch - 'A' + 10,
        else => return error.InvalidHexDigit,
    };
}

pub fn urlDecodeBuf(input: []const u8, buf: []u8) ![]u8 {
    std.debug.assert(buf.len >= input.len);

    var out_len: usize = 0;
    const State = enum(u2) { normal, pct_hi, pct_lo };
    var state: State = .normal;
    var hi: u8 = 0;

    for (input) |ch| {
        switch (state) {
            .normal => {
                if (ch == '%') {
                    state = .pct_hi;
                } else {
                    const out_ch: u8 = if (ch == '+') ' ' else ch;
                    buf[out_len] = out_ch;
                    out_len += 1;
                }
            },
            .pct_hi => {
                hi = (try parseHexDigit(ch)) << 4;
                state = .pct_lo;
            },
            .pct_lo => {
                buf[out_len] = hi | try parseHexDigit(ch);
                out_len += 1;
                state = .normal;
            },
        }
    }

    if (state != .normal) return error.InvalidPercentEncoding;
    return buf[0..out_len];
}
