//! Route action functions for ultar_httpd.
//!
//! httpz auto-decodes percent-encoded query values, so handlers read
//! `(try req.query()).get(name)` directly. Outbound link construction uses
//! `encodings.urlEncodeAlloc`.

const std = @import("std");
const httpz = @import("httpz");
const msgpack = @import("msgpack");

const App = @import("App.zig");
const mime = @import("mime.zig");
const mustach_render = @import("mustach_render.zig");
const IndexerWorker = @import("IndexerWorker.zig");
const urlEncodeAlloc = @import("encodings.zig").urlEncodeAlloc;

const ROWS_PER_PAGE: usize = 500;

const InvalidRangeStrError = error{
    invalid_base_offset,
    invalid_end_offset,
};

const MapFileClientError = error{
    missing_parameters,
    invalid_offset_range,
} || InvalidRangeStrError;

/// Resolve `rel` against `base_dir` into an absolute path, normalizing traversal segments.
fn buildSafePathAlloc(arena: std.mem.Allocator, base_dir: []const u8, rel: []const u8) ![]u8 {
    if (rel.len == 0) return error.InvalidPath;
    const parts = &[_][]const u8{ base_dir, rel };
    return try std.fs.path.resolve(arena, parts);
}

/// Replace any character outside `[A-Za-z0-9._+-]` with `_`.
fn sanitizeFilename(arena: std.mem.Allocator, name: []const u8) ![]u8 {
    var buf = try arena.alloc(u8, name.len);
    for (name, 0..) |ch, i| {
        buf[i] = switch (ch) {
            'a'...'z', 'A'...'Z', '0'...'9', '-', '_', '.', '+' => ch,
            else => '_',
        };
    }
    return buf;
}

/// Parse a hex `BASE..END` range string into `(base, end)`.
fn parseRangeStr(range_str: []const u8) !struct { u64, u64 } {
    var parts = std.mem.splitSequence(u8, range_str, "..");
    const base_s = parts.next() orelse return error.invalid_base_offset;
    const end_s = parts.next() orelse return error.invalid_end_offset;
    const base = std.fmt.parseInt(u64, base_s, 16) catch return error.invalid_base_offset;
    const end = std.fmt.parseInt(u64, end_s, 16) catch return error.invalid_end_offset;
    return .{ base, end };
}

const DirEntry = struct {
    typ: enum { dir, file, unindexed_tar },
    name: []const u8,
    rel_path: []const u8,
};

fn listDirectory(arena: std.mem.Allocator, io: std.Io, base_dir: []const u8, path: []const u8) ![]DirEntry {
    const full_path = if (path.len == 0)
        base_dir
    else
        try buildSafePathAlloc(arena, base_dir, path);

    var dir = try std.Io.Dir.openDirAbsolute(io, full_path, .{ .iterate = true });
    defer dir.close(io);

    var entries = try std.ArrayList(DirEntry).initCapacity(arena, 0);

    var name_list = try std.ArrayList([]const u8).initCapacity(arena, 0);
    var kind_list = try std.ArrayList(std.Io.File.Kind).initCapacity(arena, 0);
    var iter = dir.iterate();
    while (try iter.next(io)) |entry| {
        try name_list.append(arena, try arena.dupe(u8, entry.name));
        try kind_list.append(arena, entry.kind);
    }

    var name_set = std.StringHashMap(void).init(arena);
    for (name_list.items) |n| {
        try name_set.put(n, {});
    }

    for (name_list.items, kind_list.items) |name, kind| {
        if (kind == .directory) {
            const rel_path = if (path.len == 0)
                try arena.dupe(u8, name)
            else
                try std.fs.path.join(arena, &[_][]const u8{ path, name });
            try entries.append(arena, .{
                .typ = .dir,
                .name = try arena.dupe(u8, name),
                .rel_path = rel_path,
            });
        } else if (std.mem.endsWith(u8, name, ".utix")) {
            const rel_path = if (path.len == 0)
                try arena.dupe(u8, name)
            else
                try std.fs.path.join(arena, &[_][]const u8{ path, name });
            try entries.append(arena, .{
                .typ = .file,
                .name = try arena.dupe(u8, name),
                .rel_path = rel_path,
            });
        } else if (std.mem.endsWith(u8, name, ".tar")) {
            const utix_name = try std.mem.concat(arena, u8, &[_][]const u8{ name, ".utix" });
            if (!name_set.contains(utix_name)) {
                const rel_path = if (path.len == 0)
                    try arena.dupe(u8, name)
                else
                    try std.fs.path.join(arena, &[_][]const u8{ path, name });
                try entries.append(arena, .{
                    .typ = .unindexed_tar,
                    .name = try arena.dupe(u8, name),
                    .rel_path = rel_path,
                });
            }
        }
    }

    std.mem.sort(DirEntry, entries.items, {}, struct {
        fn lessThan(_: void, a: DirEntry, b: DirEntry) bool {
            return std.mem.lessThan(u8, a.name, b.name);
        }
    }.lessThan);

    return entries.toOwnedSlice(arena);
}

const UtixEntry = struct {
    str_idx: []const u8,
    iidx: u64,
    offset: u64,
    keys: [][]const u8,
    offsets: []u64,
    sizes: []u64,
    metadata: ?[]const u8 = null,
};

const UtixField = enum { none, str_idx, iidx, offset, keys, offsets, sizes, metadata, unknown };

fn utixFieldFromKey(key: []const u8) UtixField {
    if (std.mem.eql(u8, key, "str_idx")) return .str_idx;
    if (std.mem.eql(u8, key, "iidx")) return .iidx;
    if (std.mem.eql(u8, key, "offset")) return .offset;
    if (std.mem.eql(u8, key, "keys")) return .keys;
    if (std.mem.eql(u8, key, "offsets")) return .offsets;
    if (std.mem.eql(u8, key, "sizes")) return .sizes;
    if (std.mem.eql(u8, key, "metadata")) return .metadata;
    return .unknown;
}

fn writeJsonString(w: *std.Io.Writer, s: []const u8) !void {
    try w.writeByte('"');
    for (s) |c| switch (c) {
        '"' => try w.writeAll("\\\""),
        '\\' => try w.writeAll("\\\\"),
        '\n' => try w.writeAll("\\n"),
        '\r' => try w.writeAll("\\r"),
        '\t' => try w.writeAll("\\t"),
        else => if (c < 0x20) {
            try w.print("\\u{X:0>4}", .{c});
        } else {
            try w.writeByte(c);
        },
    };
    try w.writeByte('"');
}

fn writeJsonFromToken(scanner: *msgpack.Scanner, w: *std.Io.Writer, tok: msgpack.Scanner.Token) !void {
    switch (tok) {
        .nil => try w.writeAll("null"),
        .boolean => |b| try w.writeAll(if (b) "true" else "false"),
        .uint => |u| try w.print("{d}", .{u}),
        .int => |i| try w.print("{d}", .{i}),
        .float => |f| try w.print("{d}", .{f}),
        .string => |s| try writeJsonString(w, s),
        .array_begin => |len| {
            try w.writeByte('[');
            for (0..len) |i| {
                if (i != 0) try w.writeByte(',');
                try writeJsonFromToken(scanner, w, try scanner.next());
            }
            const end = try scanner.next();
            if (end != .array_end) return error.InvalidFormat;
            try w.writeByte(']');
        },
        .map_begin => |len| {
            try w.writeByte('{');
            for (0..len) |i| {
                if (i != 0) try w.writeByte(',');
                const key_tok = try scanner.next();
                switch (key_tok) {
                    .map_key => |key| try writeJsonString(w, key),
                    else => return error.InvalidFormat,
                }
                try w.writeByte(':');
                try writeJsonFromToken(scanner, w, try scanner.next());
            }
            const end = try scanner.next();
            if (end != .map_end) return error.InvalidFormat;
            try w.writeByte('}');
        },
        else => return error.InvalidFormat,
    }
}

fn jsonFromMsgpackValue(arena: std.mem.Allocator, scanner: *msgpack.Scanner, tok: msgpack.Scanner.Token) ![]const u8 {
    var w: std.Io.Writer.Allocating = .init(arena);
    try writeJsonFromToken(scanner, &w.writer, tok);
    return w.written();
}

fn parseUtixFile(arena: std.mem.Allocator, io: std.Io, file_path: []const u8) ![]UtixEntry {
    var file = try std.Io.Dir.openFileAbsolute(io, file_path, .{ .mode = .read_only });
    defer file.close(io);

    const read_buf = try arena.alloc(u8, 16384);
    var reader = file.readerStreaming(io, read_buf);
    var scanner = msgpack.Scanner.init(&reader.interface, arena);
    defer scanner.deinit();

    var entries = try std.ArrayList(UtixEntry).initCapacity(arena, 0);
    var current: ?UtixEntry = null;
    var current_field: UtixField = .none;
    var skip_depth: usize = 0;

    while (true) {
        const tok = try scanner.next();
        if (skip_depth > 0) {
            switch (tok) {
                .map_begin, .array_begin => skip_depth += 1,
                .map_end, .array_end => skip_depth -= 1,
                else => {},
            }
            continue;
        }
        switch (tok) {
            .end => break,
            .map_begin => {
                if (current_field == .metadata) {
                    if (current) |*e| {
                        e.metadata = try jsonFromMsgpackValue(arena, &scanner, tok);
                    }
                    current_field = .none;
                    continue;
                }
                if (current_field == .unknown or current_field == .metadata) {
                    skip_depth = 1;
                    current_field = .none;
                    continue;
                }
                current = UtixEntry{
                    .str_idx = "",
                    .iidx = 0,
                    .offset = 0,
                    .keys = &[_][]const u8{},
                    .offsets = &[_]u64{},
                    .sizes = &[_]u64{},
                    .metadata = null,
                };
            },
            .map_key => |key| {
                current_field = utixFieldFromKey(key);
            },
            .map_end => {
                if (current) |entry| {
                    try entries.append(arena, entry);
                    current = null;
                }
            },
            .array_begin => |len| {
                if (current_field == .unknown) {
                    skip_depth = 1;
                    current_field = .none;
                    continue;
                }
                if (current == null) continue;
                if (current_field == .keys) {
                    var tmp = try std.ArrayList([]const u8).initCapacity(arena, len);
                    var i: usize = 0;
                    while (i < len) : (i += 1) {
                        const it = try scanner.next();
                        switch (it) {
                            .string => |s| try tmp.append(arena, try arena.dupe(u8, s)),
                            else => return error.InvalidFormat,
                        }
                    }
                    _ = try scanner.next();
                    current.?.keys = try tmp.toOwnedSlice(arena);
                } else if (current_field == .offsets or current_field == .sizes) {
                    var tmp = try std.ArrayList(u64).initCapacity(arena, len);
                    var i: usize = 0;
                    while (i < len) : (i += 1) {
                        const it = try scanner.next();
                        switch (it) {
                            .uint => |u| try tmp.append(arena, u),
                            .int => |iv| try tmp.append(arena, @intCast(iv)),
                            else => return error.InvalidFormat,
                        }
                    }
                    _ = try scanner.next();
                    if (current_field == .offsets) {
                        current.?.offsets = try tmp.toOwnedSlice(arena);
                    } else {
                        current.?.sizes = try tmp.toOwnedSlice(arena);
                    }
                } else {
                    var i: usize = 0;
                    while (i < len) : (i += 1) {
                        _ = try scanner.next();
                    }
                    _ = try scanner.next();
                }
            },
            .string => |s| {
                if (current == null) continue;
                if (current_field == .str_idx) {
                    current.?.str_idx = try arena.dupe(u8, s);
                }
            },
            .uint => |u| {
                if (current == null) continue;
                switch (current_field) {
                    .iidx => current.?.iidx = u,
                    .offset => current.?.offset = u,
                    else => {},
                }
            },
            .int => |iv| {
                if (current == null) continue;
                switch (current_field) {
                    .iidx => current.?.iidx = @intCast(iv),
                    .offset => current.?.offset = @intCast(iv),
                    else => {},
                }
            },
            else => {},
        }
    }

    return entries.toOwnedSlice(arena);
}

fn renderBrowseHtml(
    arena: std.mem.Allocator,
    io: std.Io,
    template_cache: anytype,
    base_dir: []const u8,
    dir_param: []const u8,
) ![]const u8 {
    const entries = try listDirectory(arena, io, base_dir, dir_param);

    const show_parent = dir_param.len > 0;
    const parent: []const u8 = if (show_parent)
        (std.fs.path.dirname(dir_param) orelse "")
    else
        "";

    var items = try std.ArrayList(mustach_render.FileListItem).initCapacity(arena, entries.len);
    const enc_dir_param = try urlEncodeAlloc(arena, dir_param);
    for (entries) |entry| {
        const enc_rel = try urlEncodeAlloc(arena, entry.rel_path);
        try items.append(arena, .{
            .is_dir = entry.typ == .dir,
            .is_unindexed_tar = entry.typ == .unindexed_tar,
            .name = entry.name,
            .rel_path_enc = enc_rel,
            .dir_enc = enc_dir_param,
        });
    }

    const list_tpl = try template_cache.get("ultar_httpd/templates/file_list.html");
    var w: std.Io.Writer.Allocating = .init(arena);
    try mustach_render.renderFileList(arena, list_tpl, .{
        .show_parent = show_parent,
        .parent = parent,
        .entries = items.items,
    }, &w.writer);
    return w.written();
}

fn renderLoadHtml(
    arena: std.mem.Allocator,
    io: std.Io,
    template_cache: anytype,
    base_dir: []const u8,
    path_param: []const u8,
    page: usize,
) ![]const u8 {
    const full_path = try buildSafePathAlloc(arena, base_dir, path_param);

    std.Io.Dir.accessAbsolute(io, full_path, .{}) catch
        return try arena.dupe(u8, "<p>File not found</p>");

    const entries = parseUtixFile(arena, io, full_path) catch
        return try arena.dupe(u8, "<p>Error parsing index file</p>");

    if (entries.len == 0)
        return try arena.dupe(u8, "<p>No items found in the index file.</p>");

    const tar_path = if (std.mem.endsWith(u8, path_param, ".utix"))
        path_param[0 .. path_param.len - 5]
    else
        path_param;

    const total_rows = entries.len;
    const total_pages = (total_rows + ROWS_PER_PAGE - 1) / ROWS_PER_PAGE;
    const clamped_page = @min(page, total_pages - 1);
    const start = clamped_page * ROWS_PER_PAGE;
    const end = @min(start + ROWS_PER_PAGE, total_rows);
    const page_entries = entries[start..end];

    var key_set = std.StringHashMap(void).init(arena);
    defer key_set.deinit();
    for (entries) |e| {
        for (e.keys) |k| {
            if (!key_set.contains(k)) try key_set.put(k, {});
        }
    }

    var all_keys_list = try std.ArrayList([]const u8).initCapacity(arena, key_set.count());
    var it = key_set.iterator();
    while (it.next()) |kv| {
        try all_keys_list.append(arena, kv.key_ptr.*);
    }
    std.mem.sort([]const u8, all_keys_list.items, {}, struct {
        fn lessThan(_: void, a: []const u8, b: []const u8) bool {
            return std.mem.lessThan(u8, a, b);
        }
    }.lessThan);

    var headers = try std.ArrayList(mustach_render.LoadTableHeader).initCapacity(arena, all_keys_list.items.len);
    for (all_keys_list.items) |k| {
        try headers.append(arena, .{ .name = k, .enc_name = try urlEncodeAlloc(arena, k) });
    }

    const enc_file = try urlEncodeAlloc(arena, tar_path);
    const enc_utix_file = try urlEncodeAlloc(arena, path_param);

    var rows = try std.ArrayList(mustach_render.LoadTableRow).initCapacity(arena, page_entries.len);
    for (page_entries) |entry| {
        var per_row = std.StringHashMap(usize).init(arena);
        defer per_row.deinit();
        for (entry.keys, 0..) |k, kidx| {
            try per_row.put(k, kidx);
        }

        var cells = try std.ArrayList(mustach_render.LoadTableCell).initCapacity(arena, all_keys_list.items.len);
        for (all_keys_list.items) |k| {
            if (per_row.get(k)) |kidx| {
                if (kidx < entry.offsets.len and kidx < entry.sizes.len) {
                    const offset = entry.offset + entry.offsets[kidx];
                    const size = entry.sizes[kidx];
                    const range_str = try std.fmt.allocPrint(arena, "{X:0>8}..{X:0>8}", .{ offset, offset + size });
                    try cells.append(arena, .{ .has = true, .range_str = range_str });
                } else {
                    try cells.append(arena, .{ .has = false, .range_str = "" });
                }
            } else {
                try cells.append(arena, .{ .has = false, .range_str = "" });
            }
        }

        const id_value = if (entry.str_idx.len > 0) entry.str_idx else try std.fmt.allocPrint(arena, "{X}", .{entry.iidx});
        try rows.append(arena, .{
            .cells = cells.items,
            .id_value = id_value,
            .row_idx = @intCast(entry.iidx),
            .has_metadata = entry.metadata != null,
            .metadata_json = entry.metadata,
        });
    }

    var page_links = try std.ArrayList(mustach_render.LoadTablePageLink).initCapacity(arena, total_pages);
    for (0..total_pages) |p| {
        try page_links.append(arena, .{
            .num = try std.fmt.allocPrint(arena, "{d}", .{p + 1}),
            .url = try std.fmt.allocPrint(arena, "/load?file={s}&page={d}", .{ enc_utix_file, p }),
            .is_current = p == clamped_page,
        });
    }

    const has_pages = total_pages > 1;
    const has_prev = clamped_page > 0;
    const has_next = clamped_page + 1 < total_pages;
    const prev_url: []const u8 = if (has_prev)
        try std.fmt.allocPrint(arena, "/load?file={s}&page={d}", .{ enc_utix_file, clamped_page - 1 })
    else
        "";
    const next_url: []const u8 = if (has_next)
        try std.fmt.allocPrint(arena, "/load?file={s}&page={d}", .{ enc_utix_file, clamped_page + 1 })
    else
        "";
    const total_rows_str = try std.fmt.allocPrint(arena, "{d}", .{total_rows});

    const load_tpl = try template_cache.get("ultar_httpd/templates/load_table.html");
    var w: std.Io.Writer.Allocating = .init(arena);
    try mustach_render.renderLoadTable(arena, load_tpl, .{
        .enc_file = enc_file,
        .headers = headers.items,
        .rows = rows.items,
        .tar_path = tar_path,
        .total_rows = total_rows_str,
        .has_pages = has_pages,
        .has_prev = has_prev,
        .has_next = has_next,
        .prev_url = prev_url,
        .next_url = next_url,
        .pages = page_links.items,
    }, &w.writer);
    return w.written();
}

pub fn indexRoot(app: *App, req: *httpz.Request, res: *httpz.Response) !void {
    const arena = req.arena;
    const q = try req.query();

    const initial_dir = q.get("dir") orelse "";
    const initial_file = q.get("file") orelse "";
    const page = std.fmt.parseInt(usize, q.get("page") orelse "0", 10) catch 0;

    const browse_html = renderBrowseHtml(arena, app.io, app.template_cache, app.base_dir, initial_dir) catch "";

    const table_html: []const u8 = if (initial_file.len > 5 and std.mem.endsWith(u8, initial_file, ".utix"))
        renderLoadHtml(arena, app.io, app.template_cache, app.base_dir, initial_file, page) catch ""
    else
        "";

    const tpl = try app.template_cache.get("ultar_httpd/templates/base.html");
    var w: std.Io.Writer.Allocating = .init(arena);
    try mustach_render.renderBase(arena, tpl, .{
        .body = browse_html,
        .table_body = table_html,
    }, &w.writer);

    res.content_type = .HTML;
    res.body = w.written();
}

pub fn browse(app: *App, req: *httpz.Request, res: *httpz.Response) !void {
    const arena = req.arena;
    const q = try req.query();
    const dir_param = q.get("dir") orelse "";
    const html = try renderBrowseHtml(arena, app.io, app.template_cache, app.base_dir, dir_param);
    res.content_type = .HTML;
    res.body = html;
}

pub fn load(app: *App, req: *httpz.Request, res: *httpz.Response) !void {
    const arena = req.arena;
    const q = try req.query();
    const path_param = q.get("file") orelse (q.get("path") orelse "");

    res.content_type = .HTML;
    if (path_param.len == 0) {
        res.body = "<p>No path specified</p>";
        return;
    }

    const page = std.fmt.parseInt(usize, q.get("page") orelse "0", 10) catch 0;
    const html = try renderLoadHtml(arena, app.io, app.template_cache, app.base_dir, path_param, page);
    res.body = html;
}

pub fn staticAsset(app: *App, req: *httpz.Request, res: *httpz.Response) !void {
    const arena = req.arena;
    // httpz's `/*` glob does not populate a named param; extract the tail by stripping the static prefix.
    const path = req.url.path;
    const static_prefix = "/static/";
    if (!std.mem.startsWith(u8, path, static_prefix)) {
        res.status = 404;
        res.body = "Not Found";
        return;
    }
    const tail = path[static_prefix.len..];

    if (tail.len == 0) {
        res.status = 404;
        res.body = "Not Found";
        return;
    }

    // Reject `..` / empty segments and restrict each segment to `[A-Za-z0-9._-]` to prevent traversal out of /static/.
    var seg_it = std.mem.splitScalar(u8, tail, '/');
    while (seg_it.next()) |seg| {
        if (seg.len == 0 or std.mem.eql(u8, seg, "..")) {
            res.status = 404;
            res.body = "Not Found";
            return;
        }
        for (seg) |ch| {
            switch (ch) {
                'a'...'z', 'A'...'Z', '0'...'9', '-', '_', '.' => {},
                else => {
                    res.status = 404;
                    res.body = "Not Found";
                    return;
                },
            }
        }
    }

    const full_path = try std.fs.path.join(arena, &[_][]const u8{ "ultar_httpd/static", tail });

    const data = app.template_cache.get(full_path) catch {
        res.status = 404;
        res.body = "Not Found";
        return;
    };

    const ext = std.fs.path.extension(tail);
    res.header("Content-Type", mime.forStaticExt(ext));
    res.header("Cache-Control", "public, max-age=3600");
    res.body = data;
}

pub fn indexRequest(app: *App, req: *httpz.Request, res: *httpz.Response) !void {
    const arena = req.arena;
    const q = try req.query();
    const file_param = q.get("file") orelse "";

    if (file_param.len == 0) {
        res.status = 400;
        res.body = "missing file parameter";
        return;
    }

    if (!std.mem.endsWith(u8, file_param, ".tar")) {
        res.status = 400;
        res.body = "not a tar file";
        return;
    }

    const abs_path = try buildSafePathAlloc(arena, app.base_dir, file_param);

    std.Io.Dir.accessAbsolute(app.io, abs_path, .{}) catch {
        res.status = 404;
        res.body = "file not found";
        return;
    };

    app.indexer_worker.enqueue(abs_path, file_param) catch {
        res.status = 500;
        res.body = "failed to enqueue";
        return;
    };

    const jobs = try app.indexer_worker.getStatus(arena);
    const html = try renderIndexingPanel(arena, jobs);
    res.content_type = .HTML;
    res.body = html;
}

pub fn indexStatus(app: *App, req: *httpz.Request, res: *httpz.Response) !void {
    const arena = req.arena;
    const jobs = try app.indexer_worker.getStatus(arena);

    // Headers must be set before the body is written, so tally before rendering.
    var active: usize = 0;
    var done: usize = 0;
    for (jobs) |job| {
        if (job.status == .queued or job.status == .running) active += 1;
        if (job.status == .done) done += 1;
    }
    if (active == 0 and done >= 1) {
        res.header("HX-Trigger", "indexingDone");
    }

    const html = try renderIndexingPanel(arena, jobs);
    res.content_type = .HTML;
    res.body = html;
}

pub fn mapFile(app: *App, req: *httpz.Request, res: *httpz.Response) !void {
    mapFileImpl(app, req, res) catch |err| switch (err) {
        MapFileClientError.missing_parameters,
        InvalidRangeStrError.invalid_base_offset,
        InvalidRangeStrError.invalid_end_offset,
        MapFileClientError.invalid_offset_range,
        => {
            res.status = 400;
            res.body = "Bad Request";
        },
        else => return err,
    };
}

fn mapFileImpl(app: *App, req: *httpz.Request, res: *httpz.Response) !void {
    const arena = req.arena;
    const q = try req.query();
    const file_param = q.get("file") orelse "";
    const range_str = q.get("range_str") orelse "";
    const k_param = q.get("k") orelse "";
    const id_param: ?[]const u8 = q.get("id");

    if (file_param.len == 0 or range_str.len <= 2 or k_param.len == 0) {
        return MapFileClientError.missing_parameters;
    }

    const base_offset, const end_offset = try parseRangeStr(range_str);
    if (end_offset <= base_offset) return MapFileClientError.invalid_offset_range;

    const full_path = try buildSafePathAlloc(arena, app.base_dir, file_param);

    var file = try std.Io.Dir.openFileAbsolute(app.io, full_path, .{});
    defer file.close(app.io);

    const file_size = (try file.stat(app.io)).size;
    if (end_offset > file_size) return MapFileClientError.invalid_offset_range;

    const read_buf = try arena.alloc(u8, 16384);
    var reader = file.reader(app.io, read_buf);
    try reader.seekTo(base_offset);
    const data = try reader.interface.readAlloc(arena, end_offset - base_offset);

    res.header("Content-Type", mime.forFileExt(k_param));

    if (id_param) |id_val| {
        const cleaned_id = try sanitizeFilename(arena, id_val);
        const filename: []const u8 = if (k_param.len > 0 and k_param[0] == '.')
            try std.fmt.allocPrint(arena, "{s}{s}", .{ cleaned_id, k_param })
        else if (k_param.len > 0)
            try std.fmt.allocPrint(arena, "{s}.{s}", .{ cleaned_id, k_param })
        else
            try arena.dupe(u8, cleaned_id);
        const header_val = try std.fmt.allocPrint(arena, "attachment; filename=\"{s}\"", .{filename});
        res.header("Content-Disposition", header_val);
    }

    res.body = data;
}

/// Render the indexing-status panel fragment; HTMX attributes here are load-bearing for client polling.
fn renderIndexingPanel(arena: std.mem.Allocator, jobs: []const IndexerWorker.JobSnapshot) ![]const u8 {
    var has_active = false;
    for (jobs) |job| {
        if (job.status == .queued or job.status == .running) {
            has_active = true;
            break;
        }
    }

    var html = try std.ArrayList(u8).initCapacity(arena, 512);

    if (has_active) {
        try html.appendSlice(arena, "<div id=\"indexing-panel\" hx-get=\"/index/status\" hx-trigger=\"every 2s\" hx-swap=\"outerHTML\">");
    } else {
        try html.appendSlice(arena, "<div id=\"indexing-panel\">");
    }

    for (jobs) |job| {
        const fname = if (std.mem.lastIndexOfScalar(u8, job.rel_path, '/')) |idx|
            job.rel_path[idx + 1 ..]
        else
            job.rel_path;

        const pct: u64 = switch (job.status) {
            .done => 100,
            .running => if (job.bytes_total > 0) job.bytes_scanned * 100 / job.bytes_total else 0,
            .@"error" => if (job.bytes_total > 0) job.bytes_scanned * 100 / job.bytes_total else 0,
            .queued => 0,
        };

        const cls: []const u8 = switch (job.status) {
            .done => " done",
            .@"error" => " error",
            else => "",
        };

        const pct_str = try std.fmt.allocPrint(arena, "{d}", .{pct});

        try html.appendSlice(arena, "<div class=\"idx-bar");
        try html.appendSlice(arena, cls);
        try html.appendSlice(arena, "\"><div class=\"idx-fill\" style=\"width:");
        try html.appendSlice(arena, pct_str);
        try html.appendSlice(arena, "%\"></div><span class=\"idx-label\">");

        switch (job.status) {
            .queued => {
                try html.appendSlice(arena, fname);
                try html.appendSlice(arena, " (queued)");
            },
            .running => {
                try html.appendSlice(arena, fname);
                try html.appendSlice(arena, " ");
                try html.appendSlice(arena, pct_str);
                try html.appendSlice(arena, "%");
            },
            .done => {
                try html.appendSlice(arena, "\xe2\x9c\x93 "); // ✓
                try html.appendSlice(arena, fname);
            },
            .@"error" => {
                try html.appendSlice(arena, "\xe2\x9c\x97 "); // ✗
                try html.appendSlice(arena, fname);
                if (job.error_msg) |msg| {
                    try html.appendSlice(arena, ": ");
                    try html.appendSlice(arena, msg);
                }
            },
        }

        try html.appendSlice(arena, "</span></div>");
    }

    try html.appendSlice(arena, "</div>");
    return html.items;
}
