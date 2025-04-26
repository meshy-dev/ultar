const std = @import("std");

const Counter = std.atomic.Value(usize);

pub const EnqueueError = error{Full};
pub const DequeueError = error{Empty};

fn SPSCRing(comptime capacity: comptime_int, comptime T: type) type {
    return struct {
        const Self = @This();
        comptime {
            // Power-of-2 check
            const cap_plus_1 = capacity + 1;
            if ((cap_plus_1 & capacity) != 0) {
                @panic("Capacity must be a (power of 2) minus one");
            }
        }
        const mask = capacity;

        head: Counter align(std.atomic.cache_line),
        tail: Counter align(std.atomic.cache_line),
        buffer: [capacity + 1]T = undefined,

        pub fn init() Self {
            return .{ .head = Counter.init(0), .tail = Counter.init(0) };
        }

        pub fn enqueue(self: *Self, item: T) EnqueueError!void {
            const cur_tail = self.tail.load(.unordered);
            const next_tail = (cur_tail + 1) & mask;
            if (next_tail == self.head.load(.acquire)) {
                return EnqueueError.Full;
            }
            self.buffer[cur_tail] = item;
            self.tail.store(next_tail, .release);
        }

        pub fn dequeue(self: *Self) DequeueError!T {
            const cur_head = self.head.load(.unordered);
            if (cur_head == self.tail.load(.acquire)) {
                return DequeueError.Empty;
            }
            const item = self.buffer[cur_head];
            self.head.store((cur_head + 1) & mask, .release);
            return item;
        }
    };
}

test "test SPSC enqueue-dequeue" {
    var ring = SPSCRing(3, u32).init();
    try ring.enqueue(1);
    try ring.enqueue(2);
    try ring.enqueue(3);
    try std.testing.expectError(EnqueueError.Full, ring.enqueue(5));
    try std.testing.expectEqual(1, try ring.dequeue());
    try std.testing.expectEqual(2, try ring.dequeue());
    try std.testing.expectEqual(3, try ring.dequeue());
    try std.testing.expectError(DequeueError.Empty, ring.dequeue());
}
