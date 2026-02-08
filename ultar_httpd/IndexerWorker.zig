// IndexerWorker: in-process indexer that runs in a dedicated thread.
//
// HTTP handlers enqueue jobs via a mutex-protected queue; the worker thread
// picks them up, creates an xev loop, and runs the TarFileScanner +
// WdsIndexingState logic from indexer.zig in-process.

const std = @import("std");
const xev = @import("xev");
const indexer_mod = @import("indexer");

const WdsIndexingState = indexer_mod.WdsIndexingState;
const Indexer = indexer_mod.Indexer;

const Self = @This();

const logger = std.log.scoped(.IndexerWorker);

pub const JobStatus = enum {
    queued,
    running,
    done,
    @"error",
};

pub const Job = struct {
    abs_path: []const u8, // absolute path to tar file (owned)
    rel_path: []const u8, // relative path for display (owned)
    status: JobStatus,
    error_msg: ?[]const u8 = null, // owned, if present
};

/// Snapshot of a job returned to HTTP handlers (allocated on caller's arena).
pub const JobSnapshot = struct {
    rel_path: []const u8,
    status: JobStatus,
    error_msg: ?[]const u8,
};

allocator: std.mem.Allocator,
mutex: std.Thread.Mutex = .{},
cond: std.Thread.Condition = .{},
thread: ?std.Thread = null,
running: bool = false,

// All jobs (history + active). Protected by mutex.
jobs: std.ArrayListUnmanaged(Job) = .{},
// Indices into `jobs` that are still queued. Protected by mutex.
queue: std.ArrayListUnmanaged(usize) = .{},

pub fn init(allocator: std.mem.Allocator) Self {
    return .{ .allocator = allocator };
}

pub fn deinit(self: *Self) void {
    // Signal the thread to stop
    {
        self.mutex.lock();
        defer self.mutex.unlock();
        self.running = false;
        self.cond.signal();
    }

    // Join the thread
    if (self.thread) |t| {
        t.join();
        self.thread = null;
    }

    // Free owned strings
    for (self.jobs.items) |*job| {
        self.allocator.free(job.abs_path);
        self.allocator.free(job.rel_path);
        if (job.error_msg) |msg| self.allocator.free(msg);
    }
    self.jobs.deinit(self.allocator);
    self.queue.deinit(self.allocator);
}

pub fn start(self: *Self) !void {
    self.running = true;
    self.thread = try std.Thread.spawn(.{}, workerLoop, .{self});
}

/// Enqueue a tar file for indexing. Strings are duped and owned by the worker.
pub fn enqueue(self: *Self, abs_path: []const u8, rel_path: []const u8) !void {
    self.mutex.lock();
    defer self.mutex.unlock();

    // Check for duplicate (same abs_path not yet done/errored)
    for (self.jobs.items) |*job| {
        if (std.mem.eql(u8, job.abs_path, abs_path)) {
            if (job.status == .queued or job.status == .running) {
                return; // already queued or running
            }
        }
    }

    const owned_abs = try self.allocator.dupe(u8, abs_path);
    const owned_rel = try self.allocator.dupe(u8, rel_path);

    const job_idx = self.jobs.items.len;
    try self.jobs.append(self.allocator, .{
        .abs_path = owned_abs,
        .rel_path = owned_rel,
        .status = .queued,
    });
    try self.queue.append(self.allocator, job_idx);
    self.cond.signal();
}

/// Return a snapshot of all jobs. Allocated on the provided arena.
pub fn getStatus(self: *Self, arena: std.mem.Allocator) ![]JobSnapshot {
    self.mutex.lock();
    defer self.mutex.unlock();

    var snapshots = try std.ArrayListUnmanaged(JobSnapshot).initCapacity(arena, self.jobs.items.len);
    for (self.jobs.items) |*job| {
        try snapshots.append(arena, .{
            .rel_path = try arena.dupe(u8, job.rel_path),
            .status = job.status,
            .error_msg = if (job.error_msg) |msg| try arena.dupe(u8, msg) else null,
        });
    }
    return snapshots.items;
}

fn workerLoop(self: *Self) void {
    logger.info("IndexerWorker thread started", .{});

    while (true) {
        // Wait for work
        var batch: []usize = &.{};
        {
            self.mutex.lock();
            defer self.mutex.unlock();

            while (self.running and self.queue.items.len == 0) {
                self.cond.wait(&self.mutex);
            }

            if (!self.running) {
                logger.info("IndexerWorker thread stopping", .{});
                return;
            }

            // Drain the queue into a local batch
            batch = self.queue.toOwnedSlice(self.allocator) catch {
                logger.err("Failed to drain queue", .{});
                continue;
            };
        }
        defer self.allocator.free(batch);

        // Process each job
        for (batch) |job_idx| {
            self.processJob(job_idx);
        }
    }
}

fn processJob(self: *Self, job_idx: usize) void {
    // Mark as running
    const abs_path: []const u8 = blk: {
        self.mutex.lock();
        defer self.mutex.unlock();
        self.jobs.items[job_idx].status = .running;
        break :blk self.jobs.items[job_idx].abs_path;
    };

    logger.info("Indexing: {s}", .{abs_path});

    // Build output path: abs_path ++ ".utix"
    const out_path = std.mem.concat(self.allocator, u8, &[_][]const u8{ abs_path, ".utix" }) catch {
        self.setJobError(job_idx, "out of memory");
        return;
    };
    defer self.allocator.free(out_path);

    // Create output file
    const out_file = std.fs.createFileAbsolute(out_path, .{ .truncate = true }) catch |err| {
        const msg = std.fmt.allocPrint(self.allocator, "failed to create output: {}", .{err}) catch "create error";
        self.setJobError(job_idx, msg);
        return;
    };
    // out_file is closed by WdsIndexingState.deinit -> OStream.deinit

    // Create xev loop for this job
    var loop = xev.Loop.init(.{}) catch |err| {
        const msg = std.fmt.allocPrint(self.allocator, "loop init failed: {}", .{err}) catch "loop error";
        self.setJobError(job_idx, msg);
        out_file.close();
        return;
    };
    defer loop.deinit();

    // Init indexing state
    var state = WdsIndexingState.init(self.allocator, &loop, out_file, .msgpack) catch |err| {
        const msg = std.fmt.allocPrint(self.allocator, "state init failed: {}", .{err}) catch "init error";
        self.setJobError(job_idx, msg);
        return;
    };

    // Init scanner
    var scanner = Indexer.initFp(state, abs_path) catch |err| {
        const msg = std.fmt.allocPrint(self.allocator, "scanner init failed: {}", .{err}) catch "scanner error";
        self.setJobError(job_idx, msg);
        state.deinit();
        return;
    };
    scanner.enqueueRead(&loop);

    // Run the event loop
    loop.run(.until_done) catch |err| {
        const msg = std.fmt.allocPrint(self.allocator, "loop run failed: {}", .{err}) catch "run error";
        self.setJobError(job_idx, msg);
        scanner.state.deinit();
        scanner.deinit();
        return;
    };

    // Clean up scanner resources
    scanner.state.deinit();
    scanner.deinit();

    // Mark as done
    {
        self.mutex.lock();
        defer self.mutex.unlock();
        self.jobs.items[job_idx].status = .done;
    }

    logger.info("Indexing complete: {s}", .{abs_path});
}

fn setJobError(self: *Self, job_idx: usize, msg: []const u8) void {
    self.mutex.lock();
    defer self.mutex.unlock();
    self.jobs.items[job_idx].status = .@"error";
    // If the message was allocated by us (not a static string), store it; otherwise dupe it.
    self.jobs.items[job_idx].error_msg = self.allocator.dupe(u8, msg) catch null;
}
