//! Entry point for `ultar_httpd`: wires up singletons and runs the httpz server until SIGINT/SIGTERM.

const std = @import("std");
const clap = @import("clap");
const httpz = @import("httpz");

const App = @import("App.zig");
const handlers = @import("handlers.zig");
const TemplateCache = @import("TemplateCache.zig");
const IndexerWorker = @import("IndexerWorker.zig");

/// Shared between `main` and `shutdown`; non-null only while the server is listening.
var server_instance: ?*httpz.Server(*App) = null;

fn shutdown(_: std.posix.SIG) callconv(.c) void {
    if (server_instance) |srv| srv.stop();
}

/// Drop trailing path separators, preserving a lone leading `/`.
fn trimTrailingSep(path: []const u8) []const u8 {
    var end: usize = path.len;
    while (end > 1 and path[end - 1] == std.fs.path.sep) {
        end -= 1;
    }
    return path[0..end];
}

/// Resolve the base directory from `--data`, `DATA_PATH`, or cwd. Returned slice is owned by `gpa`.
fn resolveBaseDir(gpa: std.mem.Allocator, io: std.Io, data_flag: ?[]const u8, env_data_path: ?[]const u8) ![]const u8 {
    if (data_flag) |data_dir| {
        const abs = if (std.fs.path.isAbsolute(data_dir))
            try gpa.dupe(u8, data_dir)
        else
            try std.fs.path.resolve(gpa, &[_][]const u8{ ".", data_dir });
        defer gpa.free(abs);
        return try gpa.dupe(u8, trimTrailingSep(abs));
    }

    if (env_data_path) |env_path| {
        const abs = if (std.fs.path.isAbsolute(env_path))
            try gpa.dupe(u8, env_path)
        else
            try std.fs.path.resolve(gpa, &[_][]const u8{ ".", env_path });
        defer gpa.free(abs);
        return try gpa.dupe(u8, trimTrailingSep(abs));
    }

    var cwd_buf: [std.fs.max_path_bytes]u8 = undefined;
    const cwd_len = try std.Io.Dir.cwd().realPath(io, &cwd_buf);
    return try gpa.dupe(u8, trimTrailingSep(cwd_buf[0..cwd_len]));
}

/// Map `--addr` to `httpz.Config.Address`; unknown values warn and bind to `.all`.
fn pickAddress(addr_flag: []const u8, port: u16) httpz.Config.Address {
    if (std.mem.eql(u8, addr_flag, "0.0.0.0")) return httpz.Config.Address.all(port);
    if (std.mem.eql(u8, addr_flag, "127.0.0.1")) return httpz.Config.Address.localhost(port);
    if (std.mem.eql(u8, addr_flag, "localhost")) return httpz.Config.Address.localhost(port);
    std.log.warn("unsupported --addr '{s}'; binding to 0.0.0.0", .{addr_flag});
    return httpz.Config.Address.all(port);
}

pub fn main(init: std.process.Init) !void {
    var stderr_buffer: [1024]u8 = undefined;
    var stderr_writer = std.Io.File.stderr().writer(init.io, &stderr_buffer);
    const stderr = &stderr_writer.interface;
    defer stderr.flush() catch {};

    const params = comptime clap.parseParamsComptime(
        \\-h, --help           Display this help and exit.
        \\--addr <STR>         Bind address (default 0.0.0.0).
        \\-p, --port <PORT>     Port to listen on (default 3000).
        \\-d, --data <DIR>     Data root directory (overrides DATA_PATH).
        \\-t, --threads <INT>  Number of threads (default 4).
        \\
    );
    const parsers = comptime .{
        .STR = clap.parsers.string,
        .PORT = clap.parsers.int(u16, 10),
        .INT = clap.parsers.int(u32, 10),
        .DIR = clap.parsers.string,
    };
    var diag = clap.Diagnostic{};
    var res = clap.parse(clap.Help, &params, parsers, init.minimal.args, .{
        .diagnostic = &diag,
        .allocator = init.gpa,
    }) catch |err| {
        diag.report(stderr, err) catch {};
        return err;
    };
    defer res.deinit();

    if (res.args.help != 0)
        return clap.help(stderr, clap.Help, &params, .{});

    const addr_flag = res.args.addr orelse "0.0.0.0";
    const port: u16 = res.args.port orelse 3000;
    const threads: u32 = res.args.threads orelse 4;

    std.log.info("Launching with addr={s}:{} data={s} threads={d}", .{ addr_flag, port, res.args.data orelse "?", threads });

    const env_data_path = init.environ_map.get("DATA_PATH");
    const base_dir = try resolveBaseDir(init.gpa, init.io, res.args.data, env_data_path);
    defer init.gpa.free(base_dir);
    std.log.info("Serving base dir: {s}", .{base_dir});

    var template_cache: TemplateCache = undefined;
    try template_cache.init(init.gpa);
    defer template_cache.deinit();

    var indexer_worker: IndexerWorker = undefined;
    try indexer_worker.init(init.gpa);
    defer indexer_worker.deinit();

    var app = App{
        .gpa = init.gpa,
        .io = init.io,
        .base_dir = base_dir,
        .template_cache = &template_cache,
        .indexer_worker = &indexer_worker,
    };

    var server = try httpz.Server(*App).init(init.io, init.gpa, .{
        .address = pickAddress(addr_flag, port),
        // httpz max_conn is u16; cap at its max.
        .workers = .{ .count = 1, .max_conn = 65535 },
        .thread_pool = .{ .count = @intCast(threads) },
    }, &app);
    defer server.deinit();

    var router = try server.router(.{});
    router.get("/", handlers.indexRoot, .{});
    router.get("/browse", handlers.browse, .{});
    router.get("/load", handlers.load, .{});
    router.get("/static/*", handlers.staticAsset, .{});
    router.post("/index", handlers.indexRequest, .{});
    router.get("/index", handlers.indexRequest, .{});
    router.get("/index/status", handlers.indexStatus, .{});
    router.get("/map_file", handlers.mapFile, .{});

    var sigact: std.posix.Sigaction = .{
        .handler = .{ .handler = shutdown },
        .mask = std.posix.sigemptyset(),
        .flags = 0,
    };
    std.posix.sigaction(.INT, &sigact, null);
    std.posix.sigaction(.TERM, &sigact, null);

    server_instance = &server;
    defer server_instance = null;

    std.log.info("Listening on {s}:{d} with {d} threads", .{ addr_flag, port, threads });
    try server.listen();
}
