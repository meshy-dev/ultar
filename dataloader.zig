const std = @import("std");
const xev = @import("xev");

const concurrent_ring = @import("concurrent_ring.zig");

const logger = std.log.scoped(.dataloader);
const wlog = std.log.scoped(.data_worker);

const FileHandle = packed struct {
    idx: u10,
    generation: u14,
    path_checksum: u8,
};

const max_file_slots = std.math.maxInt(@FieldType(FileHandle, "idx"));
const max_generation = std.math.maxInt(@FieldType(FileHandle, "generation"));

const Request = union(enum) {
    open_file: struct {
        file_path: []const u8,
    },
    close_file: struct {
        file: u32,
    },
    read_block: struct {
        base: u64,
        size: u32,
        file: u32,
    },
    drain: struct {},
};

const ResponsePayload = union(enum) {
    open_file: FileHandle,
    close_file: struct {},
    read_block: struct {
        data: []const u8,
        size: u32,
        file: u32,
    },
};

const Response = struct {
    request_id: u64,
    payload: LoaderError!ResponsePayload,
};

const LoaderError = error{
    TooManyOpenFiles,
} || std.fs.File.OpenError;

const gpa = std.heap.c_allocator;

fn path_checksum(path: []const u8) u8 {
    var checksum: u8 = 0;
    for (path) |c| {
        checksum ^= c;
    }
    return checksum;
}

const LoaderCtx = struct {
    const Self = @This();

    request_ring: concurrent_ring.SPSCRing(15, Request),
    result_ring: concurrent_ring.SPSCRing(15, Response),

    file_slots: [max_file_slots]?std.fs.File = @splat(null),
    xfile_slots: [max_file_slots]xev.File = undefined,
    file_slots_generation: [max_file_slots]u32 = std.mem.zeroes([max_file_slots]u32),
    file_slots_checksum: [max_file_slots]u8 = undefined,

    loop: xev.Loop,
    worker_thread: ?std.Thread = null,

    req_cnt: u64 = 0,
    is_running: bool = false,
    is_draining: bool = false,

    debug_max_req_id: u64 = std.math.maxInt(u64),
    tick: u64 = 0,
    debug_max_tick: u64 = std.math.maxInt(u64),

    fn find_free_file_slot(self: *Self) !FileHandle {
        // This isn't called too often, just linear scan
        for (self.file_slots, 0..) |f, i| {
            if (f == null) {
                self.file_slots_generation[i] += 1;
                return .{
                    .idx = @intCast(i),
                    .generation = 0,
                    .path_checksum = 0,
                };
            }
        }

        return LoaderError.TooManyOpenFiles;
    }

    fn send_response(self: *Self, req_id: u64, resp: LoaderError!ResponsePayload) void {
        while (true) {
            self.result_ring.enqueue(.{
                .request_id = req_id,
                .payload = resp,
            }) catch {
                std.Thread.yield() catch {};
                std.atomic.spinLoopHint();
                continue;
            };

            break;
        }
    }

    fn handle_req(self: *Self, req_id: u64, req: Request) void {
        switch (req) {
            .open_file => |open_req| {
                const file_path = open_req.file_path;

                var h = self.find_free_file_slot() catch |err| {
                    self.send_response(req_id, err);
                    return;
                };

                // Open the file
                const f = std.fs.cwd().openFile(open_req.file_path, .{ .mode = .read_only }) catch |err| {
                    self.send_response(req_id, err);
                    return;
                };
                const xf = xev.File.init(f) catch unreachable;

                // Commit state
                const slot: usize = @intCast(h.idx);
                h.path_checksum = path_checksum(file_path);
                self.file_slots[slot] = f;
                self.xfile_slots[slot] = xf;
                self.file_slots_generation[slot] += 1; // gen 0 is reserved to catch errors
                self.file_slots_checksum[slot] = h.path_checksum;
                const gen = self.file_slots_generation[slot];
                if (gen > max_generation) @panic("Open file generation overflow");
                h.generation = @intCast(gen);

                self.send_response(req_id, .{ .open_file = h });
            },

            .drain => {
                self.is_draining = true;
            },

            else => @panic("Not implemented"),
        }

        self.worker_thread = null;
    }

    fn run_worker(self: *Self) void {
        while (self.is_running) {
            self.tick += 1;
            if (self.tick > self.debug_max_tick) {
                @panic("Tick overflow");
            }

            if (self.request_ring.dequeue()) |req| {
                const req_id = self.req_cnt;
                self.req_cnt += 1;

                if (self.req_cnt > self.debug_max_req_id) {
                    @panic("Request ID overflow");
                }

                wlog.debug("Req {}: {s}", .{ req_id, @tagName(req) });

                self.handle_req(req_id, req);
            } else if (self.is_draining) {
                wlog.debug("No more requests, draining event loop", .{});

                // If we are draining & no new requests, run the loop and exit
                self.loop.run(.until_done) catch |err| {
                    std.debug.panic("Error in event loop: {}", .{err});
                };
                self.is_running = false;
                break;
            }

            self.loop.run(.no_wait) catch |err| {
                std.debug.panic("Error in event loop: {}", .{err});
            };
        }
    }

    // Loader side functions
    fn drainResponse(self: *Self) void {
        var count: usize = 0;
        while (self.result_ring.dequeue()) |_| {
            std.atomic.spinLoopHint();
            count += 1;
        }
        if (count > 0) {
            logger.debug("Drained {} responses", .{count});
        }
    }

    pub fn sendSynced(self: *Self, req: Request) void {
        while (true) {
            self.request_ring.enqueue(req) catch {
                std.atomic.spinLoopHint();
                std.Thread.yield() catch {};
                continue;
            };

            break;
        }
    }

    pub fn trySend(self: *Self, req: Request) bool {
        self.request_ring.enqueue(req) catch {
            return false;
        };
        return true;
    }

    pub fn join(self: *Self) void {
        logger.debug("Joining worker thread", .{});
        const thread = self.worker_thread orelse @panic("Worker thread not started");
        // Drain response so that pending requests won't block us from sending drain
        while (!self.trySend(.{ .drain = .{} })) {
            self.drainResponse();
            std.Thread.yield() catch {};
        }
        // Now wait until the worker thread is done
        while (self.worker_thread != null) {
            self.drainResponse();
            std.Thread.yield() catch {};
        }
        // When worker thread is null, it means the thread has exited
        self.drainResponse();
        thread.join();
    }

    pub fn start(self: *Self) !void {
        if (self.worker_thread) |_| {
            @panic("Worker thread already started");
        }

        self.is_running = true;

        self.worker_thread = try std.Thread.spawn(.{
            .stack_size = 1 * 1024 * 1024,
            .allocator = gpa,
        }, Self.run_worker, .{self});

        logger.debug("Worker thread started", .{});
    }

    pub fn init() !Self {
        return .{
            .request_ring = concurrent_ring.SPSCRing(15, Request).init(),
            .result_ring = concurrent_ring.SPSCRing(15, Response).init(),
            .file_slots = [_]?std.fs.File{null} ** max_file_slots,
            .file_slots_generation = [_]u32{0} ** max_file_slots,
            .loop = try xev.Loop.init(.{}),
        };
    }
};

test "test dataloader join" {
    std.testing.log_level = .debug;

    var ctx = try LoaderCtx.init();
    ctx.debug_max_req_id = 2;
    ctx.debug_max_tick = 10;
    try ctx.start();

    ctx.sendSynced(.{ .open_file = .{ .file_path = "/sys/does_not_exist" } });
    ctx.join();

    try std.testing.expect(ctx.is_running == false);
    try std.testing.expectEqual(null, ctx.result_ring.dequeue());
}

test "test dataloader blocked join" {
    std.testing.log_level = .debug;

    var ctx = try LoaderCtx.init();
    ctx.debug_max_req_id = 100;
    ctx.debug_max_tick = 1000;
    try ctx.start();

    // Send until we can't send anymore (blocked on full response ring)
    while (ctx.trySend(.{ .open_file = .{ .file_path = "/sys/does_not_exist" } })) {}
    for (0..20) |_| {
        // For good measure try to send a few more
        _ = ctx.trySend(.{ .open_file = .{ .file_path = "/sys/does_not_exist" } });
    }
    // This must unblock the worker
    ctx.join();

    try std.testing.expect(ctx.is_running == false);
    try std.testing.expectEqual(null, ctx.result_ring.dequeue());
}
