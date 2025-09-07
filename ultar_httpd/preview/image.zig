const std = @import("std");

pub fn render(alloc: std.mem.Allocator, src: []const u8) ![]const u8 {
    return std.fmt.allocPrint(
        alloc,
        "<div style=\"margin-top:6px\"><img src=\"{s}\" alt=\"preview\" style=\"max-width:512px; max-height:384px; border:1px solid var(--border);\"/></div>",
        .{src},
    );
}
