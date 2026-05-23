const std = @import("std");
const xev = @import("xev");
const indexer_mod = @import("indexer");

const WdsIndexingState = indexer_mod.WdsIndexingState;
const Indexer = indexer_mod.Indexer;

const Self = @This();
const logger = std.log.scoped(.IndexerWorker);

// kqueue (macOS) dispatches regular-file I/O to a thread pool; without one
// every read returns EPERM. io_uring (Linux) handles file I/O in-kernel.
const needs_thread_pool = @import("builtin").os.tag != .linux;
const ThreadPoolStorage = if (needs_thread_pool) xev.ThreadPool else void;

pub const JobStatus = enum { queued, running, done, @"error" };

/// Heap-owned per-tar record. Lives in `jobs` from submission until the next
/// `getStatus` purge after completion.
pub const Job = struct {
    abs_path: []const u8,
    rel_path: []const u8,
    status: JobStatus,
    error_msg: ?[]const u8 = null,
    bytes_total: u64 = 0,
    bytes_scanned: usize = 0,
    done_event: std.Io.Event = .unset,
    scanner: ?*Indexer = null,
    worker: *Self,
};

pub const JobSnapshot = struct {
    rel_path: []const u8,
    status: JobStatus,
    error_msg: ?[]const u8,
    bytes_scanned: u64 = 0,
    bytes_total: u64 = 0,
};

allocator: std.mem.Allocator,
threaded: std.Io.Threaded,
io: std.Io,
mutex: std.Io.Mutex,
group: std.Io.Group,

xev_thread: std.Thread,
thread_pool: ThreadPoolStorage,
loop: xev.Loop,
notify_async: xev.Async,
notify_completion: xev.Completion,

/// All submitted jobs. Protected by `mutex`.
jobs: std.ArrayListUnmanaged(*Job),
/// Jobs awaiting `startJob` on the xev thread. Protected by `mutex`.
pending: std.ArrayListUnmanaged(*Job),
/// Completed jobs whose scanner must be freed on the xev thread. Protected by `mutex`.
to_cleanup: std.ArrayListUnmanaged(*Job),
active: usize,
shutdown: bool,

/// Initialize in place. Spawns the xev thread; caller must pair with `deinit`.
pub fn init(self: *Self, allocator: std.mem.Allocator) !void {
    self.allocator = allocator;
    self.mutex = .init;
    self.group = .init;
    self.jobs = .empty;
    self.pending = .empty;
    self.to_cleanup = .empty;
    self.active = 0;
    self.shutdown = false;

    self.threaded = .init(allocator, .{});
    errdefer self.threaded.deinit();
    self.io = self.threaded.io();

    if (needs_thread_pool) {
        self.thread_pool = .init(.{});
    } else {
        self.thread_pool = {};
    }
    errdefer if (needs_thread_pool) {
        self.thread_pool.shutdown();
        self.thread_pool.deinit();
    };

    self.loop = try xev.Loop.init(.{
        .thread_pool = if (needs_thread_pool) &self.thread_pool else null,
    });
    errdefer self.loop.deinit();

    self.notify_async = try xev.Async.init();
    errdefer self.notify_async.deinit();
    self.notify_async.wait(&self.loop, &self.notify_completion, Self, self, onNotify);

    self.xev_thread = try std.Thread.spawn(.{}, xevLoop, .{self});
}

/// Awaits in-flight submitters, joins the xev thread, frees all registered jobs.
pub fn deinit(self: *Self) void {
    self.group.await(self.io) catch {};

    {
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        self.shutdown = true;
    }
    self.notify_async.notify() catch {};
    self.xev_thread.join();

    self.notify_async.deinit();
    self.loop.deinit();
    if (needs_thread_pool) {
        self.thread_pool.shutdown();
        self.thread_pool.deinit();
    }

    for (self.jobs.items) |job| {
        self.allocator.free(job.abs_path);
        self.allocator.free(job.rel_path);
        if (job.error_msg) |msg| self.allocator.free(msg);
        self.allocator.destroy(job);
    }
    self.jobs.deinit(self.allocator);
    self.pending.deinit(self.allocator);
    self.to_cleanup.deinit(self.allocator);

    self.threaded.deinit();
}

/// Schedule a tar file for indexing. Path slices are copied; caller retains its own.
pub fn enqueue(self: *Self, abs_path: []const u8, rel_path: []const u8) !void {
    {
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        if (self.shutdown) return error.Shutdown;
    }

    const owned_abs = try self.allocator.dupe(u8, abs_path);
    errdefer self.allocator.free(owned_abs);
    const owned_rel = try self.allocator.dupe(u8, rel_path);
    errdefer self.allocator.free(owned_rel);

    try self.group.concurrent(self.io, runJob, .{ self, owned_abs, owned_rel });
}

/// Snapshot all jobs into `arena`, then purge completed entries from `jobs`.
pub fn getStatus(self: *Self, arena: std.mem.Allocator) ![]JobSnapshot {
    self.mutex.lockUncancelable(self.io);
    defer self.mutex.unlock(self.io);

    var snapshots = try std.ArrayListUnmanaged(JobSnapshot).initCapacity(arena, self.jobs.items.len);
    for (self.jobs.items) |job| {
        const bs = @atomicLoad(usize, &job.bytes_scanned, .monotonic);
        try snapshots.append(arena, .{
            .rel_path = try arena.dupe(u8, job.rel_path),
            .status = job.status,
            .error_msg = if (job.error_msg) |msg| try arena.dupe(u8, msg) else null,
            .bytes_scanned = bs,
            .bytes_total = job.bytes_total,
        });
    }

    var i: usize = self.jobs.items.len;
    while (i > 0) {
        i -= 1;
        const job = self.jobs.items[i];
        if (job.status != .done and job.status != .@"error") continue;
        // Scanner is already freed on the xev thread before status transitions to .done;
        // .@"error" paths never leave a live scanner attached.
        self.allocator.free(job.abs_path);
        self.allocator.free(job.rel_path);
        if (job.error_msg) |msg| self.allocator.free(msg);
        self.allocator.destroy(job);
        _ = self.jobs.orderedRemove(i);
    }

    return snapshots.items;
}

/// Submitter task body. Takes ownership of `owned_abs`/`owned_rel`; blocks
/// the submitter until the scanner signals `done_event`.
fn runJob(self: *Self, owned_abs: []const u8, owned_rel: []const u8) void {
    // Job is heap-allocated so `&bytes_scanned` and `&done_event` stay stable
    // for the scanner across `jobs` ArrayList growth.
    const job = self.allocator.create(Job) catch {
        self.allocator.free(owned_abs);
        self.allocator.free(owned_rel);
        return;
    };
    job.* = .{
        .abs_path = owned_abs,
        .rel_path = owned_rel,
        .status = .queued,
        .worker = self,
    };

    const registered = register: {
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);

        // Dedup against any still-active job for the same path.
        for (self.jobs.items) |existing| {
            if (std.mem.eql(u8, existing.abs_path, owned_abs) and
                (existing.status == .queued or existing.status == .running))
            {
                break :register false;
            }
        }

        self.jobs.append(self.allocator, job) catch break :register false;
        self.pending.append(self.allocator, job) catch {
            _ = self.jobs.pop();
            break :register false;
        };
        break :register true;
    };

    if (!registered) {
        self.allocator.free(owned_abs);
        self.allocator.free(owned_rel);
        self.allocator.destroy(job);
        return;
    }

    self.notify_async.notify() catch {};
    job.done_event.waitUncancelable(self.io);
}

fn xevLoop(self: *Self) void {
    self.loop.run(.until_done) catch |err| {
        logger.err("xev loop error: {}", .{err});
    };
}

fn onNotify(self_opt: ?*Self, l: *xev.Loop, c: *xev.Completion, r: xev.Async.WaitError!void) xev.CallbackAction {
    _ = c;
    _ = r catch {};
    const self = self_opt.?;

    var pending_drain: []*Job = &.{};
    var cleanup_drain: []*Job = &.{};
    {
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        if (self.pending.items.len > 0) {
            pending_drain = self.pending.toOwnedSlice(self.allocator) catch &.{};
        }
        if (self.to_cleanup.items.len > 0) {
            cleanup_drain = self.to_cleanup.toOwnedSlice(self.allocator) catch &.{};
        }
    }

    for (cleanup_drain) |job| {
        if (job.scanner) |sc| {
            sc.state.deinit();
            sc.deinit(self.io);
            self.allocator.destroy(sc);
            job.scanner = null;
        }
        self.mutex.lockUncancelable(self.io);
        if (job.status == .running) job.status = .done;
        self.active -= 1;
        self.mutex.unlock(self.io);
        job.done_event.set(self.io);
    }
    if (cleanup_drain.len > 0) self.allocator.free(cleanup_drain);

    for (pending_drain) |job| {
        self.startJob(l, job) catch |err| self.markError(job, err);
    }
    if (pending_drain.len > 0) self.allocator.free(pending_drain);

    var stop_loop = false;
    {
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        if (self.shutdown and self.active == 0 and
            self.pending.items.len == 0 and self.to_cleanup.items.len == 0)
        {
            stop_loop = true;
        }
    }
    if (stop_loop) {
        l.stop();
        return .disarm;
    }
    return .rearm;
}

/// Build a `WdsIndexingState` writing to `<abs_path>.utix`. On success the
/// output file is owned by the returned state.
fn createIndexingState(self: *Self, l: *xev.Loop, abs_path: []const u8) !WdsIndexingState {
    const out_path_abs = try std.mem.concat(self.allocator, u8, &[_][]const u8{ abs_path, ".utix" });
    defer self.allocator.free(out_path_abs);

    const out_file = try std.Io.Dir.createFileAbsolute(self.io, out_path_abs, .{ .truncate = true });
    errdefer out_file.close(self.io);

    return try WdsIndexingState.init(self.allocator, self.io, l, out_file, .msgpack);
}

fn startJob(self: *Self, l: *xev.Loop, job: *Job) !void {
    const abs_path = job.abs_path;

    const file_size: u64 = if (std.Io.Dir.openFileAbsolute(self.io, abs_path, .{ .mode = .read_only })) |f| sz: {
        defer f.close(self.io);
        const st = f.stat(self.io) catch break :sz 0;
        break :sz st.size;
    } else |_| 0;

    {
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        job.bytes_total = file_size;
        job.status = .running;
        self.active += 1;
    }
    errdefer {
        self.mutex.lockUncancelable(self.io);
        self.active -= 1;
        self.mutex.unlock(self.io);
    }

    var state = try self.createIndexingState(l, abs_path);
    var state_owned_by_scanner = false;
    errdefer if (!state_owned_by_scanner) state.deinit();

    const scanner = try self.allocator.create(Indexer);
    errdefer self.allocator.destroy(scanner);

    scanner.* = try Indexer.initFp(self.io, state, abs_path);
    state_owned_by_scanner = true;
    errdefer {
        scanner.state.deinit();
        scanner.deinit(self.io);
    }

    scanner.state.progress_ptr = &job.bytes_scanned;
    scanner.state.done_event_ptr = &job.done_event;
    scanner.state.done_notify_cb = onJobDone;
    scanner.state.done_notify_ctx = job;

    job.scanner = scanner;
    scanner.enqueueRead(l);
}

/// Scanner terminal callback. The scanner's read completion and any in-flight
/// ostream chunk writes are still owned by io_uring at this point. Arm the
/// ostream's drain hook; cleanup is deferred until every chunk write has
/// completed (`onOstreamDrained`).
fn onJobDone(ctx: *anyopaque) void {
    const job: *Job = @ptrCast(@alignCast(ctx));
    const sc = job.scanner orelse {
        onOstreamDrained(ctx);
        return;
    };
    sc.state.ostream.drain_done_cb = onOstreamDrained;
    sc.state.ostream.drain_done_ctx = ctx;
    if (sc.state.ostream.pending_writes == 0) onOstreamDrained(ctx);
}

/// Fired once the ostream's `pending_writes` reaches zero. Still runs inside
/// a callback (writeCb or onJobDone), so we only enqueue the job for the
/// xev thread to free on a later tick.
fn onOstreamDrained(ctx: *anyopaque) void {
    const job: *Job = @ptrCast(@alignCast(ctx));
    const self = job.worker;
    {
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        self.to_cleanup.append(self.allocator, job) catch {};
    }
    self.notify_async.notify() catch {};
}

fn markError(self: *Self, job: *Job, err: anyerror) void {
    const msg = std.fmt.allocPrint(self.allocator, "indexing failed: {}", .{err}) catch null;
    self.mutex.lockUncancelable(self.io);
    job.status = .@"error";
    job.error_msg = msg;
    self.mutex.unlock(self.io);
    job.done_event.set(self.io);
}
