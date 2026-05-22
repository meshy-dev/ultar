//! Typed handler context passed to every httpz action. Owned by `main`,
//! shared by reference across all worker threads.

const std = @import("std");
const httpz = @import("httpz");

const TemplateCache = @import("TemplateCache.zig");
const IndexerWorker = @import("IndexerWorker.zig");

const App = @This();

gpa: std.mem.Allocator,
io: std.Io,
base_dir: []const u8,
template_cache: *TemplateCache,
indexer_worker: *IndexerWorker,

/// httpz lifecycle hook: logs the request and forwards to the route action.
pub fn dispatch(self: *App, action: httpz.Action(*App), req: *httpz.Request, res: *httpz.Response) !void {
    std.log.scoped(.http).info("{s} {s}", .{ @tagName(req.method), req.url.path });
    try action(self, req, res);
}

/// httpz lifecycle hook: response for unmatched routes.
pub fn notFound(self: *App, req: *httpz.Request, res: *httpz.Response) !void {
    _ = self;
    _ = req;
    res.status = 404;
    res.body = "Not Found";
}

/// httpz lifecycle hook: terminal handler for errors escaping an action.
// Returns void: body-write failures here have nowhere to propagate.
pub fn uncaughtError(self: *App, req: *httpz.Request, res: *httpz.Response, err: anyerror) void {
    _ = self;
    std.log.scoped(.http).err("uncaught error on {s}: {s}", .{ req.url.path, @errorName(err) });
    res.status = 500;
    res.body = "Internal Server Error";
}
