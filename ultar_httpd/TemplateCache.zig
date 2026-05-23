const std = @import("std");
const builtin = @import("builtin");

const TemplateCache = @This();

const templates_path = "ultar_httpd/templates";

alloc: std.mem.Allocator,
threaded: std.Io.Threaded,
io: std.Io,
mutex: std.Io.Mutex,
map: std.StringHashMapUnmanaged([]const u8),
watcher: Watcher,

/// TemplateCache.init : Initializes a TemplateCache in-place.
pub fn init(self: *TemplateCache, allocator: std.mem.Allocator) !void {
    // Direct field assignment keeps `Threaded` at its final address so the vtable pointer captured by `.io()` stays valid.
    self.alloc = allocator;
    self.mutex = .init;
    self.map = .empty;
    self.watcher = .{};
    self.threaded = .init(allocator, .{});
    errdefer self.threaded.deinit();
    self.io = self.threaded.io();

    // Best-effort: cache still works without live invalidation if the
    // platform-specific watcher fails to attach (e.g. directory missing,
    // unsupported OS).
    self.watcher.start(templates_path) catch return;

    var t = try std.Thread.spawn(.{}, TemplateCache.watchLoop, .{self});
    // macOS only lets a thread rename itself; tolerate the failure.
    t.setName(self.io, "tmpl.watch") catch {};
}

pub fn deinit(self: *TemplateCache) void {
    self.invalidateAll();
    self.map.clearAndFree(self.alloc);
    self.watcher.stop();
    self.threaded.deinit();
}

fn invalidateAll(self: *TemplateCache) void {
    self.mutex.lockUncancelable(self.io);
    defer self.mutex.unlock(self.io);
    var it = self.map.iterator();
    while (it.next()) |kv| {
        self.alloc.free(kv.key_ptr.*);
        self.alloc.free(kv.value_ptr.*);
    }
    self.map.clearRetainingCapacity();
    std.debug.print("Template cache invalidated\n", .{});
}

pub fn get(self: *TemplateCache, path: []const u8) ![]const u8 {
    self.mutex.lockUncancelable(self.io);
    defer self.mutex.unlock(self.io);
    if (self.map.get(path)) |v| {
        std.debug.print("Cache hit for {s} ({} bytes)\n", .{ path, v.len });
        return v;
    }
    const max = 2 * 1024 * 1024;
    const bytes = try std.Io.Dir.cwd().readFileAlloc(self.io, path, self.alloc, .limited(max));
    errdefer self.alloc.free(bytes);
    const key = try self.alloc.dupe(u8, path);
    errdefer self.alloc.free(key);
    try self.map.put(self.alloc, key, bytes);
    std.debug.print("Loaded {} bytes for {s} into cache\n", .{ bytes.len, path });
    return bytes;
}

/// Blocks on the OS-specific event source and invalidates the cache on each
/// event; exits when `deinit` closes the watcher fds.
fn watchLoop(self: *TemplateCache) void {
    while (self.watcher.waitForEvent()) {
        self.invalidateAll();
    }
}

/// Cross-platform directory-change watcher. `start` attaches to a path,
/// `stop` releases all OS resources, and `waitForEvent` blocks until either
/// an event is available (return `true`) or the watcher is stopped (`false`).
const Watcher = switch (builtin.os.tag) {
    .linux => LinuxWatcher,
    .macos, .ios, .tvos, .watchos, .visionos => DarwinWatcher,
    else => NullWatcher,
};

const NullWatcher = struct {
    pub fn start(_: *@This(), _: []const u8) !void {
        return error.Unsupported;
    }
    pub fn stop(_: *@This()) void {}
    pub fn waitForEvent(_: *@This()) bool {
        return false;
    }
};

const LinuxWatcher = struct {
    fd: std.posix.fd_t = -1,

    pub fn start(self: *@This(), path: []const u8) !void {
        // Blocking inotify so `waitForEvent` parks the watch thread instead
        // of busy-looping on `error.WouldBlock`.
        const fd_raw = std.os.linux.inotify_init1(0);
        if (@as(isize, @bitCast(fd_raw)) < 0) return error.InotifyInitFailed;
        const fd: std.posix.fd_t = @intCast(fd_raw);
        errdefer _ = std.os.linux.close(fd);

        var path_buf: [std.fs.max_path_bytes]u8 = undefined;
        const path_z = try std.fmt.bufPrintZ(&path_buf, "{s}", .{path});

        const mask = std.os.linux.IN.MODIFY |
            std.os.linux.IN.CREATE |
            std.os.linux.IN.DELETE |
            std.os.linux.IN.MOVED_FROM |
            std.os.linux.IN.MOVED_TO;
        const wd = std.os.linux.inotify_add_watch(fd, path_z.ptr, mask);
        if (@as(isize, @bitCast(wd)) < 0) return error.InotifyAddWatchFailed;

        self.fd = fd;
    }

    pub fn stop(self: *@This()) void {
        if (self.fd >= 0) {
            _ = std.os.linux.close(self.fd);
            self.fd = -1;
        }
    }

    pub fn waitForEvent(self: *@This()) bool {
        if (self.fd < 0) return false;
        var buf: [4096]u8 = undefined;
        const n = std.posix.read(self.fd, &buf) catch return false;
        return n > 0;
    }
};

// EVFILT_VNODE on a directory fd fires for entry-level changes (create,
// remove, rename, extend) but NOT when an existing file's contents are
// edited in-place. In practice modern editors (vim, zed, VS Code, helix)
// save via write-tmp-then-rename, which surfaces as NOTE_RENAME/NOTE_WRITE
// on the directory and is caught. Editors that overwrite in place would
// miss; if that case ever matters, switch to FSEvents (CoreServices) which
// reports per-file modifications via FSEventStreamCreate + dispatch queue.
const DarwinWatcher = struct {
    kq: std.posix.fd_t = -1,
    dir_fd: std.posix.fd_t = -1,

    // std doesn't expose the kqueue filter numbers as named values, so the
    // values from <sys/event.h> are codified inline here.
    const EVFILT_VNODE: i16 = -4;
    const EV_ADD: u16 = 0x0001;
    const EV_CLEAR: u16 = 0x0020;
    const NOTE_DELETE: u32 = 0x00000001;
    const NOTE_WRITE: u32 = 0x00000002;
    const NOTE_EXTEND: u32 = 0x00000004;
    const NOTE_RENAME: u32 = 0x00000020;

    pub fn start(self: *@This(), path: []const u8) !void {
        var path_buf: [std.fs.max_path_bytes]u8 = undefined;
        const path_z = try std.fmt.bufPrintZ(&path_buf, "{s}", .{path});

        // `O_EVTONLY` opens the dir solely for change notifications; it
        // doesn't count against the kernel's unmount-busy semantics.
        const dir_fd_c = std.c.open(path_z.ptr, .{ .EVTONLY = true, .DIRECTORY = true });
        if (dir_fd_c < 0) return error.OpenFailed;
        errdefer _ = std.c.close(dir_fd_c);

        const kq_c = std.c.kqueue();
        if (kq_c < 0) return error.KqueueFailed;
        errdefer _ = std.c.close(kq_c);

        var change: std.c.Kevent = .{
            .ident = @intCast(dir_fd_c),
            .filter = EVFILT_VNODE,
            .flags = EV_ADD | EV_CLEAR,
            .fflags = NOTE_DELETE | NOTE_WRITE | NOTE_EXTEND | NOTE_RENAME,
            .data = 0,
            .udata = 0,
        };
        const r = std.c.kevent(kq_c, @ptrCast(&change), 1, undefined, 0, null);
        if (r < 0) return error.KeventRegisterFailed;

        self.dir_fd = dir_fd_c;
        self.kq = kq_c;
    }

    pub fn stop(self: *@This()) void {
        // Close kq first so any blocking `kevent` call returns immediately.
        if (self.kq >= 0) {
            _ = std.c.close(self.kq);
            self.kq = -1;
        }
        if (self.dir_fd >= 0) {
            _ = std.c.close(self.dir_fd);
            self.dir_fd = -1;
        }
    }

    pub fn waitForEvent(self: *@This()) bool {
        if (self.kq < 0) return false;
        var ev: std.c.Kevent = undefined;
        const r = std.c.kevent(self.kq, undefined, 0, @ptrCast(&ev), 1, null);
        return r > 0;
    }
};
