const std = @import("std");
const builtin = @import("builtin");
const zap = @import("zap");
const clap = @import("clap");
const msgpack = @import("msgpack");
const htmx_embed = @import("htmx_embed");
// preview templates handled via mustache
const os = std.os;

const TemplateCache = @import("TemplateCache.zig");

const urlDecodeBuf = @import("encodings.zig").urlDecodeBuf;
const urlEncodeAlloc = @import("encodings.zig").urlEncodeAlloc;

// Global gpa
var gpa_instance = std.heap.GeneralPurposeAllocator(.{}){};
const gpa = gpa_instance.allocator();

// Base directory to browse
var base_dir_buf: [std.fs.MAX_PATH_BYTES]u8 = undefined;
var base_dir: []const u8 = undefined;

var template_cache: TemplateCache = undefined;

// Join and normalize a path under base_dir; reject traversal outside base_dir
fn buildSafePathAlloc(alloc: std.mem.Allocator, rel: []const u8) ![]u8 {
    if (rel.len == 0) return error.InvalidPath;
    // Consider all incoming paths relative to base_dir, regardless of absolute/relative form
    const parts = &[_][]const u8{ base_dir, rel };
    const resolved = try std.fs.path.resolve(alloc, parts);
    return resolved; // allocated with alloc; caller frees (arena in route handlers)
}

fn findMimeTypeAlloc(alloc: std.mem.Allocator, file_name: []const u8) ![]const u8 {
    // zig std considers .* to be no extension, however this is ultar keys
    // thus we don't use std.fs.path.extension
    var i = file_name.len;
    while (i > 0 and file_name[i - 1] != '.') {
        i -= 1;
    }
    const e = file_name[i..];

    const obj = zap.fio.http_mimetype_find(@constCast(e.ptr), e.len);

    if (obj == 0) return try alloc.dupe(u8, "application/octet-stream");

    return try zap.util.fio2strAlloc(alloc, obj);
}

// Directory entry structure
const DirEntry = struct {
    typ: enum { dir, file },
    name: []const u8,
    rel_path: []const u8,

    fn deinit(self: *const DirEntry) void {
        gpa.free(self.name);
        gpa.free(self.rel_path);
    }
};

// Utix entry structure
const UtixEntry = struct {
    str_idx: []const u8,
    iidx: u64,
    offset: u64,
    keys: [][]const u8,
    offsets: []u64,
    sizes: []u64,

    fn deinit(self: *const UtixEntry) void {
        gpa.free(self.str_idx);
        for (self.keys) |key| {
            gpa.free(key);
        }
        gpa.free(self.keys);
        gpa.free(self.offsets);
        gpa.free(self.sizes);
    }
};

const UtixField = enum { none, str_idx, iidx, offset, keys, offsets, sizes, unknown };

fn utixFieldFromKey(key: []const u8) UtixField {
    if (std.mem.eql(u8, key, "str_idx")) return .str_idx;
    if (std.mem.eql(u8, key, "iidx")) return .iidx;
    if (std.mem.eql(u8, key, "offset")) return .offset;
    if (std.mem.eql(u8, key, "keys")) return .keys;
    if (std.mem.eql(u8, key, "offsets")) return .offsets;
    if (std.mem.eql(u8, key, "sizes")) return .sizes;
    return .unknown;
}

fn trimTrailingSep(path: []const u8) []const u8 {
    var end: usize = path.len;
    while (end > 1 and path[end - 1] == std.fs.path.sep) {
        end -= 1;
    }
    return path[0..end];
}

// Initialize base directory
fn initBaseDir() !void {
    // Prefer DATA_PATH env var; fallback to current working directory
    if (std.posix.getenv("DATA_PATH")) |env_path| {
        // Resolve to absolute path if possible
        const env_slice: []const u8 = std.mem.sliceTo(env_path, 0);
        const abs = if (std.fs.path.isAbsolute(env_slice))
            env_slice
        else blk: {
            const abs_join = try std.fs.path.resolve(gpa, &[_][]const u8{ ".", env_slice });
            defer gpa.free(abs_join);
            break :blk abs_join;
        };
        base_dir = try gpa.dupe(u8, trimTrailingSep(abs));
        return;
    }

    var cwd_buf: [1024]u8 = undefined;
    const cwd = try std.fs.cwd().realpath(".", &cwd_buf);
    base_dir = try gpa.dupe(u8, trimTrailingSep(cwd));
}

// Parse .utix file using msgpack.Scanner
fn parseUtixFile(alloc: std.mem.Allocator, file_path: []const u8) ![]UtixEntry {
    var file = try std.fs.openFileAbsolute(file_path, .{ .mode = .read_only });
    defer file.close();

    const read_buf = try alloc.alloc(u8, 16384);
    defer alloc.free(read_buf);
    var reader = std.fs.File.readerStreaming(file, read_buf);
    var scanner = msgpack.Scanner.init(&reader.interface, alloc);
    defer scanner.deinit();

    var entries = try std.ArrayList(UtixEntry).initCapacity(alloc, 0);
    var current: ?UtixEntry = null;
    var current_field: UtixField = .none;

    while (true) {
        const tok = try scanner.next();
        switch (tok) {
            .end => break,
            .map_begin => |_| {
                current = UtixEntry{
                    .str_idx = "",
                    .iidx = 0,
                    .offset = 0,
                    .keys = &[_][]const u8{},
                    .offsets = &[_]u64{},
                    .sizes = &[_]u64{},
                };
            },
            .map_key => |key| {
                current_field = utixFieldFromKey(key);
            },
            .map_end => {
                if (current) |entry| {
                    try entries.append(alloc, entry);
                    current = null;
                }
            },
            .array_begin => |len| {
                if (current == null) continue;
                if (current_field == .keys) {
                    var tmp = try std.ArrayList([]const u8).initCapacity(alloc, len);
                    var i: usize = 0;
                    while (i < len) : (i += 1) {
                        const it = try scanner.next();
                        switch (it) {
                            .string => |s| try tmp.append(alloc, try alloc.dupe(u8, s)),
                            else => return error.InvalidFormat,
                        }
                    }
                    // consume array_end
                    _ = try scanner.next();
                    current.?.keys = try tmp.toOwnedSlice(alloc);
                } else if (current_field == .offsets or current_field == .sizes) {
                    var tmp = try std.ArrayList(u64).initCapacity(alloc, len);
                    var i: usize = 0;
                    while (i < len) : (i += 1) {
                        const it = try scanner.next();
                        switch (it) {
                            .uint => |u| try tmp.append(alloc, u),
                            .int => |iv| try tmp.append(alloc, @intCast(iv)),
                            else => return error.InvalidFormat,
                        }
                    }
                    // consume array_end
                    _ = try scanner.next();
                    if (current_field == .offsets) {
                        current.?.offsets = try tmp.toOwnedSlice(alloc);
                    } else {
                        current.?.sizes = try tmp.toOwnedSlice(alloc);
                    }
                } else {
                    // Skip unknown arrays
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
                    current.?.str_idx = try alloc.dupe(u8, s);
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

    return entries.toOwnedSlice(alloc);
}

// List directories and .utix files
fn listDirectory(alloc: std.mem.Allocator, path: []const u8) ![]DirEntry {
    // Build safe full path
    const full_path = if (std.mem.eql(u8, path, ""))
        base_dir
    else
        try buildSafePathAlloc(alloc, path);
    defer if (!std.mem.eql(u8, path, "")) alloc.free(full_path);

    // Open directory
    var dir = try std.fs.openDirAbsolute(full_path, .{ .iterate = true });
    defer dir.close();

    var entries = try std.ArrayList(DirEntry).initCapacity(alloc, 0);

    // Iterate through directory entries
    var iter = dir.iterate();
    while (try iter.next()) |entry| {
        if (entry.kind == .directory) {
            const rel_path = if (std.mem.eql(u8, path, ""))
                try alloc.dupe(u8, entry.name)
            else
                try std.fs.path.join(alloc, &[_][]const u8{ path, entry.name });

            try entries.append(alloc, .{
                .typ = .dir,
                .name = try alloc.dupe(u8, entry.name),
                .rel_path = rel_path,
            });
        } else if (std.mem.endsWith(u8, entry.name, ".utix")) {
            const rel_path = if (std.mem.eql(u8, path, ""))
                try alloc.dupe(u8, entry.name)
            else
                try std.fs.path.join(alloc, &[_][]const u8{ path, entry.name });

            try entries.append(alloc, .{
                .typ = .file,
                .name = try alloc.dupe(u8, entry.name),
                .rel_path = rel_path,
            });
        }
    }

    // Sort entries
    std.mem.sort(DirEntry, entries.items, {}, struct {
        fn lessThan(_: void, a: DirEntry, b: DirEntry) bool {
            return std.mem.lessThan(u8, a.name, b.name);
        }
    }.lessThan);

    return entries.toOwnedSlice(alloc);
}

pub fn dumpError(r: zap.Request, trace: ?*std.builtin.StackTrace) void {
    var slices = r.getParamSlices();
    while (slices.next()) |param| {
        std.debug.print("Param: {s}={s}\n", .{ param.name, param.value });
    }
    if (trace) |t| {
        std.debug.dumpStackTrace(t.*);
    }
}

pub fn onRequest(r: zap.Request) anyerror!void {
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const alloc = arena.allocator();

    // Handle different routes
    if (r.path) |path| {
        if (std.mem.eql(u8, path, "/")) {
            handleIndex(alloc, r) catch {
                dumpError(r, @errorReturnTrace());
                r.sendBody("Internal Server Error") catch {};
            };
        } else if (std.mem.startsWith(u8, path, "/browse")) {
            handleBrowse(alloc, r) catch {
                dumpError(r, @errorReturnTrace());
                r.sendBody("Internal Server Error") catch {};
            };
        } else if (std.mem.startsWith(u8, path, "/load")) {
            handleLoad(alloc, r) catch {
                dumpError(r, @errorReturnTrace());
                r.sendBody("Internal Server Error") catch {};
            };
        } else if (std.mem.startsWith(u8, path, "/map_file")) {
            handleMapFile(alloc, r) catch |err| {
                dumpError(r, @errorReturnTrace());
                switch (err) {
                    MapFileClientError.missing_parameters,
                    InvalidRangeStrError.invalid_base_offset,
                    InvalidRangeStrError.invalid_end_offset,
                    MapFileClientError.invalid_offset_range,
                    => {
                        r.setStatus(.bad_request);
                        r.sendBody("Bad Request") catch {};
                    },
                    else => {
                        r.setStatus(.internal_server_error);
                        r.sendBody("Internal Server Error") catch {};
                    },
                }
            };
        } else if (std.mem.startsWith(u8, path, "/boxed_file")) {
            handleBoxedFile(alloc, r) catch |err| {
                dumpError(r, @errorReturnTrace());
                switch (err) {
                    BoxedFileClientError.missing_parameters,
                    InvalidRangeStrError.invalid_base_offset,
                    InvalidRangeStrError.invalid_end_offset,
                    => {
                        r.setStatus(.bad_request);
                        r.sendBody("Bad Request") catch {};
                    },
                    else => {
                        r.setStatus(.internal_server_error);
                        r.sendBody("Internal Server Error") catch {};
                    },
                }
            };
        } else {
            r.setStatus(.not_found);
            r.sendBody("Not Found") catch {};
        }
    } else {
        r.setStatus(.bad_request);
        r.sendBody("Bad Request") catch {};
    }
}

fn handleIndex(arena: std.mem.Allocator, r: zap.Request) !void {
    // Always render with an empty list and trigger browse after load
    const dir_raw = r.getParamSlice("dir") orelse "";
    const initial_dir = try urlDecodeBuf(dir_raw, try arena.alloc(u8, dir_raw.len));
    const file_raw = r.getParamSlice("file") orelse "";
    const initial_file = try urlDecodeBuf(file_raw, try arena.alloc(u8, file_raw.len));
    const enc_initial_dir = try urlEncodeAlloc(arena, initial_dir);
    var loader = try std.fmt.allocPrint(arena, "<ul class=\"nav-list\" hx-get=\"/browse?dir={s}\" hx-trigger=\"load\" hx-target=\"#file-tree-list\" hx-swap=\"innerHTML\"></ul>", .{enc_initial_dir});
    if (initial_file.len > 5 and std.mem.endsWith(u8, initial_file, ".utix")) {
        const enc_initial_file = try urlEncodeAlloc(arena, initial_file);
        const table_loader = try std.fmt.allocPrint(arena, "<div style=\"display:none\" hx-get=\"/load?file={s}\" hx-trigger=\"load\" hx-target=\"#table-container\" hx-swap=\"innerHTML\"></div>", .{enc_initial_file});
        const combined = try std.fmt.allocPrint(arena, "{s}{s}", .{ loader, table_loader });
        loader = combined;
    }

    const tpl = try template_cache.get("ultar_httpd/templates/base.html");
    var mustache = try zap.Mustache.fromData(tpl);
    defer mustache.deinit();

    const data = .{ .body = loader, .htmx_js = htmx_embed.htmx_js };
    var built = mustache.build(data);
    defer built.deinit();
    const s = built.str() orelse return error.Unexpected;
    try r.sendBody(s);
}

fn handleBrowse(arena: std.mem.Allocator, r: zap.Request) !void {
    // Parse directory parameter
    const dir_raw = r.getParamSlice("dir") orelse "";
    const dir_param = try urlDecodeBuf(dir_raw, try arena.alloc(u8, dir_raw.len));

    const entries = try listDirectory(arena, dir_param);

    const Parent = struct { show: bool, parent: []const u8 };
    const parent_info: Parent = .{ .show = dir_param.len > 0, .parent = if (dir_param.len > 0) (std.fs.path.dirname(dir_param) orelse "") else "" };

    const Item = struct { is_dir: bool, name: []const u8, rel_path_enc: []const u8, dir_enc: []const u8 };
    var items = try std.ArrayList(Item).initCapacity(arena, entries.len);
    const enc_dir_param = try urlEncodeAlloc(arena, dir_param);
    for (entries) |entry| {
        const enc_rel = try urlEncodeAlloc(arena, entry.rel_path);
        try items.append(arena, .{ .is_dir = entry.typ == .dir, .name = entry.name, .rel_path_enc = enc_rel, .dir_enc = enc_dir_param });
    }

    const list_tpl = try template_cache.get("ultar_httpd/templates/file_list.html");
    var mustache = try zap.Mustache.fromData(list_tpl);
    defer mustache.deinit();

    const data = .{ .show_parent = parent_info.show, .parent = parent_info.parent, .entries = items.items };
    var built = mustache.build(data);
    defer built.deinit();
    const s = built.str() orelse return error.Unexpected;
    try r.sendBody(s);
}

fn handleLoad(arena: std.mem.Allocator, r: zap.Request) !void {
    // Parse query parameter: support new 'file' and legacy 'path'
    const raw_file = r.getParamSlice("file") orelse (r.getParamSlice("path") orelse "");
    const path_param = try urlDecodeBuf(raw_file, try arena.alloc(u8, raw_file.len));

    if (path_param.len == 0) {
        try r.sendBody("<p>No path specified</p>");
        return;
    }

    // Build full path to .utix file
    const full_path = try buildSafePathAlloc(arena, path_param);

    // Check if file exists
    std.fs.accessAbsolute(full_path, .{}) catch {
        try r.sendBody("<p>File not found</p>");
        return;
    };

    // Parse the .utix file
    const entries = parseUtixFile(arena, full_path) catch {
        std.debug.print("Error parsing .utix file\n", .{});
        try r.sendBody("<p>Error parsing index file</p>");
        return;
    };

    if (entries.len == 0) {
        try r.sendBody("<p>No items found in the index file.</p>");
        return;
    }

    // Get tar file path (remove .utix extension)
    const tar_path = if (std.mem.endsWith(u8, path_param, ".utix"))
        path_param[0 .. path_param.len - 5]
    else
        path_param;

    // Compute union of keys across entries
    var key_set = std.StringHashMap(void).init(arena);
    defer key_set.deinit();
    for (entries) |e| {
        for (e.keys) |k| {
            if (!key_set.contains(k)) try key_set.put(k, {});
        }
    }

    // Build a stable ordered list of keys (sorted)
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

    const Header = struct { name: []const u8 };
    const Cell = struct { has: bool, k: []const u8, range_str: []const u8 };
    const Row = struct { cells: []const Cell, id_value: []const u8, row_idx: i64 };

    var headers = try std.ArrayList(Header).initCapacity(arena, all_keys_list.items.len);
    for (all_keys_list.items) |k| {
        try headers.append(arena, .{ .name = k });
    }

    const enc_file = try urlEncodeAlloc(arena, tar_path);

    var rows = try std.ArrayList(Row).initCapacity(arena, entries.len);
    for (entries) |entry| {
        var per_row = std.StringHashMap(usize).init(arena);
        defer per_row.deinit();
        for (entry.keys, 0..) |k, kidx| {
            try per_row.put(k, kidx);
        }

        var cells = try std.ArrayList(Cell).initCapacity(arena, all_keys_list.items.len);
        for (all_keys_list.items) |k| {
            if (per_row.get(k)) |kidx| {
                if (kidx < entry.offsets.len and kidx < entry.sizes.len) {
                    const offset = entry.offset + entry.offsets[kidx];
                    const size = entry.sizes[kidx];
                    const range_str = try std.fmt.allocPrint(arena, "{X:0>8}..{X:0>8}", .{ offset, offset + size });
                    const enc_k = try urlEncodeAlloc(arena, k);
                    try cells.append(arena, .{ .has = true, .k = enc_k, .range_str = range_str });
                } else {
                    try cells.append(arena, .{ .has = false, .k = "", .range_str = "" });
                }
            } else {
                try cells.append(arena, .{ .has = false, .k = "", .range_str = "" });
            }
        }

        const id_value = if (entry.str_idx.len > 0) entry.str_idx else try std.fmt.allocPrint(arena, "{X}", .{entry.iidx});
        try rows.append(arena, .{ .cells = cells.items, .id_value = id_value, .row_idx = @intCast(entry.iidx) });
    }

    const load_tpl = try template_cache.get("ultar_httpd/templates/load_table.html");
    var mustache = try zap.Mustache.fromData(load_tpl);
    defer mustache.deinit();

    const data = .{ .enc_file = enc_file, .headers = headers.items, .rows = rows.items, .tar_path = tar_path };
    var built = mustache.build(data);
    defer built.deinit();
    const s = built.str() orelse return error.Unexpected;
    try r.sendBody(s);
}

fn sanitizeFilename(alloc: std.mem.Allocator, name: []const u8) ![]u8 {
    var buf = try alloc.alloc(u8, name.len);
    var i: usize = 0;
    for (name) |ch| {
        buf[i] = switch (ch) {
            'a'...'z', 'A'...'Z', '0'...'9', '-', '_', '.', '+' => ch,
            else => '_',
        };
        i += 1;
    }
    return buf;
}

const InvalidRangeStrError = error{
    invalid_base_offset,
    invalid_end_offset,
};

fn parseRangeStr(range_str: []const u8) !struct { u64, u64 } {
    var parts = std.mem.splitSequence(u8, range_str, "..");
    const base = std.fmt.parseInt(u64, parts.next() orelse return error.invalid_base_offset, 16) catch return error.invalid_base_offset;
    const end = std.fmt.parseInt(u64, parts.next() orelse return error.invalid_end_offset, 16) catch return error.invalid_end_offset;

    return .{ base, end };
}

const MapFileClientError = error{
    missing_parameters,
    invalid_offset_range,
} || InvalidRangeStrError;

fn handleMapFile(arena: std.mem.Allocator, r: zap.Request) !void {
    // Parse query parameters (decoded via zap)
    const file_raw = r.getParamSlice("file") orelse "";
    const file_param = try urlDecodeBuf(file_raw, try arena.alloc(u8, file_raw.len));
    const range_str = r.getParamSlice("range_str") orelse "";
    const k_raw = r.getParamSlice("k") orelse "";
    const k_param = try urlDecodeBuf(k_raw, try arena.alloc(u8, k_raw.len));
    const id_raw_opt = r.getParamSlice("id");
    const id_param = if (id_raw_opt) |id_raw| blk: {
        break :blk try urlDecodeBuf(id_raw, try arena.alloc(u8, id_raw.len));
    } else null;

    if (file_param.len == 0 or range_str.len <= 2 or k_param.len == 0) {
        return MapFileClientError.missing_parameters;
    }

    // Parse hex values
    const base_offset, const end_offset = try parseRangeStr(range_str);

    if (end_offset <= base_offset) {
        return MapFileClientError.invalid_offset_range;
    }

    // Build full file path
    const full_path = try buildSafePathAlloc(arena, file_param);

    // Open and memory map the file
    var file = try std.fs.openFileAbsolute(full_path, .{});
    defer file.close();

    const file_size = try file.getEndPos();
    if (end_offset > file_size) {
        return MapFileClientError.invalid_offset_range;
    }

    // Read the data
    const read_buf = try arena.alloc(u8, 16384);
    var reader = file.reader(read_buf);
    try reader.seekTo(base_offset);
    const data = try reader.interface.readAlloc(arena, end_offset - base_offset);

    // Set appropriate MIME type
    const mime_type = try findMimeTypeAlloc(arena, k_param);
    r.setHeader("Content-Type", mime_type) catch {};

    // Content-Disposition filename: {str_idx}.{key}
    if (id_param) |id_val| {
        const cleaned_id = try sanitizeFilename(arena, id_val);
        var filename: []const u8 = undefined;
        if (k_param.len > 0 and k_param[0] == '.') {
            filename = try std.fmt.allocPrint(arena, "{s}{s}", .{ cleaned_id, k_param });
        } else if (k_param.len > 0) {
            filename = try std.fmt.allocPrint(arena, "{s}.{s}", .{ cleaned_id, k_param });
        } else {
            filename = try std.fmt.allocPrint(arena, "{s}", .{cleaned_id});
        }
        const header_val = try std.fmt.allocPrint(arena, "attachment; filename=\"{s}\"", .{filename});
        r.setHeader("Content-Disposition", header_val) catch {};
    }

    // Send the data
    r.setStatus(.ok);
    try r.sendBody(data);
}

const BoxedFileClientError = error{
    missing_parameters,
    invalid_base_offset,
    invalid_end_offset,
};

fn handleBoxedFile(arena: std.mem.Allocator, r: zap.Request) !void {
    // Parse query parameters
    const file_raw = r.getParamSlice("file") orelse "";
    const file_param = try urlDecodeBuf(file_raw, try arena.alloc(u8, file_raw.len));
    const range_str_raw = r.getParamSlice("range_str") orelse "";
    const range_str_param = try urlDecodeBuf(range_str_raw, try arena.alloc(u8, range_str_raw.len));
    const k_raw = r.getParamSlice("k") orelse "";
    const k_param = try urlDecodeBuf(k_raw, try arena.alloc(u8, k_raw.len));
    const id_raw = r.getParamSlice("id") orelse "";
    const id_param = try urlDecodeBuf(id_raw, try arena.alloc(u8, id_raw.len));

    if (file_param.len == 0 or range_str_param.len <= 2 or k_param.len == 0) {
        return BoxedFileClientError.missing_parameters;
    }

    // Validate offsets
    _ = try parseRangeStr(range_str_param);

    // Decide preview based on MIME type
    const mime_type = try findMimeTypeAlloc(arena, k_param);

    // Select a partial based on mime type
    var partial_path: []const u8 = undefined;
    if (std.mem.startsWith(u8, mime_type, "image/")) {
        partial_path = "ultar_httpd/templates/boxed/image.html";
    } else if (std.mem.eql(u8, mime_type, "application/json")) {
        partial_path = "ultar_httpd/templates/boxed/json.html";
    } else if (std.mem.startsWith(u8, mime_type, "video/")) {
        partial_path = "ultar_httpd/templates/boxed/video.html";
    } else if (std.mem.startsWith(u8, mime_type, "audio/")) {
        partial_path = "ultar_httpd/templates/boxed/audio.html";
    } else if (std.mem.startsWith(u8, mime_type, "text/")) {
        partial_path = "ultar_httpd/templates/boxed/text.html";
    } else {
        partial_path = "ultar_httpd/templates/boxed/other.html";
    }

    const download_url = try std.fmt.allocPrint(arena, "/map_file?file={s}&k={s}&range_str={s}&id={s}", .{ file_param, k_param, range_str_param, id_param });

    const partial_tpl = try template_cache.get(partial_path);
    var partial = try zap.Mustache.fromData(partial_tpl);
    defer partial.deinit();
    const partial_data = .{ .mime_type = mime_type, .download_url = download_url };
    var partial_built = partial.build(partial_data);
    defer partial_built.deinit();
    const partial_html = partial_built.str() orelse return error.Unexpected;

    const boxed_tpl = try template_cache.get("ultar_httpd/templates/boxed_file.html");
    var mustache = try zap.Mustache.fromData(boxed_tpl);
    defer mustache.deinit();
    const wrapper_data = .{ .partial_html = partial_html, .mime_type = mime_type, .download_url = download_url };
    var built = mustache.build(wrapper_data);
    defer built.deinit();
    const s = built.str() orelse return error.Unexpected;
    try r.sendBody(s);
}

pub fn main() !void {
    defer _ = gpa_instance.deinit();
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const allocator = arena.allocator();

    var stderr_buffer: [1024]u8 = undefined;
    var stderr_writer = std.fs.File.stderr().writer(&stderr_buffer);
    const stderr = &stderr_writer.interface;
    defer stderr.flush() catch {};

    const params = comptime clap.parseParamsComptime(
        \\-h, --help           Display this help and exit.
        \\--addr <STR>         Bind address (default 0.0.0.0).
        \\-p, --port <INT>     Port to listen on (default 3000).
        \\-d, --data <DIR>     Data root directory (overrides DATA_PATH).
        \\-t, --threads <INT>  Number of threads (default 4).
        \\
    );
    const parsers = comptime .{
        .STR = clap.parsers.string,
        .INT = clap.parsers.int(u16, 10),
        .DIR = clap.parsers.string,
    };
    var diag = clap.Diagnostic{};
    var res = clap.parse(clap.Help, &params, parsers, .{
        .diagnostic = &diag,
        .allocator = allocator,
    }) catch |err| {
        diag.report(stderr, err) catch {};
        return err;
    };
    defer res.deinit();

    if (res.args.help != 0)
        return clap.help(stderr, clap.Help, &params, .{});

    const addr = res.args.addr orelse "0.0.0.0";
    const port: u16 = res.args.port orelse 3000;

    // Initialize base directory (override if provided)
    if (res.args.data) |data_dir| {
        const abs = if (std.fs.path.isAbsolute(data_dir))
            data_dir
        else blk: {
            const abs_join = try std.fs.path.resolve(allocator, &[_][]const u8{ ".", data_dir });
            break :blk abs_join;
        };
        base_dir = try gpa.dupe(u8, abs);
    } else {
        try initBaseDir();
    }
    defer gpa.free(base_dir);

    // Initialize template cache
    template_cache = TemplateCache.init(gpa);
    defer template_cache.deinit();
    template_cache.setupWatcher();

    var addr_buf: [1024]u8 = undefined;
    std.mem.copyForwards(u8, addr_buf[0..addr.len], addr);
    addr_buf[addr.len] = 0;

    var listener = zap.HttpListener.init(.{
        .port = port,
        .interface = &addr_buf,
        .on_request = onRequest,
        .log = true,
        .max_clients = 100000,
    });
    try listener.listen();

    std.debug.print("Listening on {s}:{d}\n", .{ addr, port });

    const threads = res.args.threads orelse 4;
    zap.start(.{
        .threads = @intCast(threads),
        .workers = 1, // 1 worker enables sharing state between threads
    });
}
