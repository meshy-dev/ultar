const std = @import("std");

pub fn octalAsciiToSize(v: []const u8) usize {
    const max_v = std.math.maxInt(usize);
    const max_digits = std.math.log2_int(usize, max_v) / 3 + 1;

    if (v.len == 0 or v.len > max_digits) {
        return 0;
    }

    var size: usize = 0;
    for (v) |c| {
        if (c == 0 or c == ' ') {
            break;
        }
        std.debug.assert(c >= '0' and c <= '7');
        const digit = c - '0';
        size = (size << 3) | digit;
    }

    return size;
}
