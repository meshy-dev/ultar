// dataloader.zig

const std = @import("std");
const xev = @import("xev");

const concurrent_ring = @import("concurrent_ring.zig");

const logger = std.log.scoped(.dataloader);
const wlog = std.log.scoped(.dataloader_io_thread);

const FileHandle = packed struct {
    idx: u10,
    generation: u14,
    path_checksum: u8,
};

const max_file_slots = std.math.maxInt(@FieldType(FileHandle, "idx"));
const max_generation = std.math.maxInt(@FieldType(FileHandle, "generation"));

const ReadBlockReq = struct {
    base: u64,
    file: FileHandle,
    result_buffer: []u8,
};

const Request = union(enum) {
    open_file: struct {
        file_path: []const u8,
    },
    close_file: FileHandle,
    read_block: ReadBlockReq,
    drain: struct {},
};

const ResponsePayload = union(enum) {
    open_file: FileHandle,
    close_file: struct {},
    read_block: struct {},
};

const Response = struct {
    request_id: u64,
    payload: LoaderError!ResponsePayload,
};

const LoaderError = error{
    TooManyOpenFiles,
    InvalidFileHandle,
    ReadError,
} || std.fs.File.OpenError || std.mem.Allocator.Error;

const gpa = std.heap.c_allocator;

fn path_checksum(path: []const u8) u8 {
    var checksum: u8 = 0;
    for (path) |c| {
        checksum +%= c;
    }
    return checksum;
}

const XevReq = struct {
    req: ReadBlockReq,
    c: xev.Completion = undefined,
    request_id: u64 = 0,
};

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

    req_mem_pool: std.heap.MemoryPool(XevReq),

    fn findFreeFileSlot(self: *Self) !FileHandle {
        // This isn't called too often, just linear scan
        for (self.file_slots, 0..) |f, i| {
            if (f == null) {
                return .{
                    .idx = @intCast(i),
                    .generation = 0,
                    .path_checksum = 0,
                };
            }
        }

        return LoaderError.TooManyOpenFiles;
    }

    fn checkFilehandle(self: *Self, file: FileHandle) !void {
        const slot: usize = @intCast(file.idx);
        if (self.file_slots[slot] == null) return LoaderError.InvalidFileHandle;
        if (self.file_slots_generation[slot] != file.generation or self.file_slots_checksum[slot] != file.path_checksum) {
            logger.warn("File handle {} is corrupted, current generation: {}, checksum: {}", .{ file, self.file_slots_generation[slot], self.file_slots_checksum[slot] });
            return LoaderError.InvalidFileHandle;
        }
    }

    fn sendResponseSynced(self: *Self, req_id: u64, resp: LoaderError!ResponsePayload) void {
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

    fn xevReadCb(
        ud: ?*Self,
        _: *xev.Loop,
        c: *xev.Completion,
        _: xev.File,
        _: xev.ReadBuffer,
        r: xev.ReadError!usize,
    ) xev.CallbackAction {
        const self = ud orelse unreachable;

        const xreq: *XevReq = @fieldParentPtr("c", c);

        const actual = r catch {
            self.sendResponseSynced(xreq.request_id, LoaderError.ReadError);
            return .disarm;
        };
        if (actual != xreq.req.result_buffer.len) {
            std.debug.panic("Unhandled error: {} != {}", .{ actual, xreq.req.result_buffer.len });
        }

        self.sendResponseSynced(xreq.request_id, .{ .read_block = .{} });

        self.req_mem_pool.destroy(xreq);
        return .disarm;
    }

    fn handleReq(self: *Self, req_id: u64, req: Request) void {
        switch (req) {
            .open_file => |open_req| {
                const file_path = open_req.file_path;
                wlog.debug("Req {}: open_file: file = {s}", .{ req_id, file_path });

                var h = self.findFreeFileSlot() catch |err| {
                    self.sendResponseSynced(req_id, err);
                    return;
                };

                // Open the file
                const f = std.fs.cwd().openFile(open_req.file_path, .{ .mode = .read_only }) catch |err| {
                    self.sendResponseSynced(req_id, err);
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

                self.sendResponseSynced(req_id, .{ .open_file = h });
            },

            .close_file => |file_handle| {
                wlog.debug("Req {}: close_file: {}", .{ req_id, file_handle });

                self.checkFilehandle(file_handle) catch |err| {
                    self.sendResponseSynced(req_id, err);
                    return;
                };

                const slot: usize = @intCast(file_handle.idx);

                // Close the file
                const f = self.file_slots[slot] orelse unreachable;
                f.close();
                self.file_slots[slot] = null;

                self.sendResponseSynced(req_id, .{ .close_file = .{} });
            },

            .read_block => |read_req| {
                wlog.debug("Req {}: read_block: file = {}, base = {}, size = {}", .{ req_id, read_req.file, read_req.base, read_req.result_buffer.len });

                self.checkFilehandle(read_req.file) catch |err| {
                    self.sendResponseSynced(req_id, err);
                    return;
                };

                const slot: usize = @intCast(read_req.file.idx);
                const xf = self.xfile_slots[slot];

                // Prepare read request
                var xreq = self.req_mem_pool.create() catch |err| {
                    self.sendResponseSynced(req_id, err);
                    return;
                };
                xreq.req = read_req;

                // Enqueue async read
                xf.pread(
                    &self.loop,
                    &xreq.c,
                    .{ .slice = read_req.result_buffer },
                    read_req.base,
                    Self,
                    self,
                    Self.xevReadCb,
                );
            },

            .drain => {
                self.is_draining = true;
            },

            // else => @panic("Not implemented"),
        }
    }

    fn run_worker(self: *Self) void {
        self.worker_thread.setName("dataloader_io_worker") catch {};

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

                self.handleReq(req_id, req);
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
            if (self.loop.active == 0) std.Thread.yield() catch {};
        }

        self.worker_thread = null;
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

    pub fn recvSynced(self: *Self) Response {
        while (true) {
            if (self.result_ring.dequeue()) |r| {
                return r;
            }

            std.Thread.yield() catch {};
        }
    }

    pub fn trySend(self: *Self, req: Request) bool {
        self.request_ring.enqueue(req) catch {
            return false;
        };
        return true;
    }

    pub fn join(self: *Self) void {
        const thread = self.worker_thread orelse @panic("Worker thread not started");
        logger.debug("Joining worker thread", .{});
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
            .req_mem_pool = try std.heap.MemoryPool(XevReq).initPreheated(gpa, 16),
        };
    }
};

test "test dataloader" {
    std.testing.log_level = .debug;

    const f = try std.fs.cwd().createFile("testfile.tar", .{});
    var rng = std.Random.DefaultPrng.init(42);
    var ref: [256]u8 = undefined;
    rng.fill(&ref);
    try f.writeAll(&ref);
    f.close();

    var ctx = try LoaderCtx.init();
    ctx.debug_max_req_id = 5;
    ctx.debug_max_tick = 1000;
    try ctx.start();

    ctx.sendSynced(.{ .open_file = .{ .file_path = "testfile.tar" } });
    const resp = try ctx.recvSynced().payload;
    const file = resp.open_file;
    var buf: [256 - 42]u8 = undefined;
    ctx.sendSynced(.{ .read_block = .{ .file = file, .base = 42, .result_buffer = &buf } });
    _ = try ctx.recvSynced().payload;
    ctx.sendSynced(.{ .close_file = file });
    ctx.join();

    try std.testing.expectEqualSlices(u8, ref[42..], &buf);
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
