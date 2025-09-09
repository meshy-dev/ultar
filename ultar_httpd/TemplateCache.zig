const std = @import("std");
const os = std.os;

const TemplateCache = @This();

alloc: std.mem.Allocator,
mutex: std.Thread.Mutex = .{},
map: std.StringHashMapUnmanaged([]const u8),
inotify_fd: std.posix.fd_t = -1,
watch_wd: i32 = -1,

pub fn init(allocator: std.mem.Allocator) TemplateCache {
    return TemplateCache{ .alloc = allocator, .map = .empty };
}

pub fn deinit(self: *TemplateCache) void {
    self.invalidateAll();
    self.map.clearAndFree(self.alloc);
    if (self.inotify_fd >= 0) {
        _ = os.linux.close(self.inotify_fd);
    }
}

pub fn setupWatcher(self: *TemplateCache) void {
    const fd_raw = os.linux.inotify_init1(os.linux.IN.NONBLOCK);
    if (fd_raw < 0) return;
    self.inotify_fd = @as(std.posix.fd_t, @intCast(fd_raw));
    const path_nt = "ultar_httpd/templates\x00";
    const wd_usize = os.linux.inotify_add_watch(@as(i32, @intCast(fd_raw)), path_nt, os.linux.IN.MODIFY | os.linux.IN.CREATE | os.linux.IN.DELETE | os.linux.IN.MOVED_FROM | os.linux.IN.MOVED_TO);
    self.watch_wd = @as(i32, @intCast(wd_usize));
    _ = std.Thread.spawn(.{}, TemplateCache.watchLoop, .{self}) catch return;
}

fn invalidateAll(self: *TemplateCache) void {
    self.mutex.lock();
    defer self.mutex.unlock();
    var it = self.map.iterator();
    while (it.next()) |kv| {
        self.alloc.free(kv.key_ptr.*);
        self.alloc.free(kv.value_ptr.*);
    }
    self.map.clearRetainingCapacity();
    std.debug.print("Template cache invalidated\n", .{});
}

pub fn get(self: *TemplateCache, path: []const u8) ![]const u8 {
    // First try under lock
    self.mutex.lock();
    defer self.mutex.unlock();
    if (self.map.get(path)) |v| {
        std.debug.print("Cache hit for {s} ({} bytes)\n", .{ path, v.len });
        return v;
    }
    const max = 2 * 1024 * 1024;
    const bytes = try std.fs.cwd().readFileAlloc(self.alloc, path, max);
    errdefer self.alloc.free(bytes);
    const key = try self.alloc.dupe(u8, path);
    errdefer self.alloc.free(key);
    try self.map.put(self.alloc, key, bytes);
    std.debug.print("Loaded {} bytes for {s} into cache\n", .{ bytes.len, path });
    return bytes;
}

pub fn watchLoop(self: *TemplateCache) void {
    var buf: [4096]u8 = undefined;
    while (true) {
        if (self.inotify_fd < 0) return;
        const n = std.posix.read(self.inotify_fd, buf[0..]) catch |e| {
            if (e == error.WouldBlock) continue;
            return;
        };
        if (n == 0) continue;
        var off: usize = 0;
        while (off + @sizeOf(os.linux.inotify_event) <= @as(usize, n)) {
            const ev_slice = buf[off .. off + @sizeOf(os.linux.inotify_event)];
            const ev_ptr = std.mem.bytesAsValue(os.linux.inotify_event, ev_slice);
            off += @sizeOf(os.linux.inotify_event) + @as(usize, @intCast(ev_ptr.len));
            self.invalidateAll();
        }
    }
}
