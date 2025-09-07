const std = @import("std");
const zap = @import("zap");
const clap = @import("clap");
const msgpack = @import("msgpack");
const htmx_embed = @import("htmx_embed");
const preview_image = @import("preview/image.zig");
const preview_json = @import("preview/json.zig");

const Html = struct {
    pub const before_body =
        \\<!doctype html>
        \\<html lang="en">
        \\<head>
        \\  <meta charset="utf-8"/>
        \\  <meta name="viewport" content="width=device-width, initial-scale=1"/>
        \\  <title>WebDataset Index Browser</title>
        \\  <meta name="color-scheme" content="dark"/>
        \\  <link rel="preconnect" href="https://fonts.googleapis.com">
        \\  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
        \\  <link href="https://fonts.googleapis.com/css2?family=SUSE&display=swap" rel="stylesheet">
        \\  <style>
        \\    :root {
        \\      color-scheme: dark;
        \\      /* Catppuccin Mocha */
        \\      --ctp-rosewater: #f5e0dc;
        \\      --ctp-flamingo: #f2cdcd;
        \\      --ctp-pink: #f5c2e7;
        \\      --ctp-mauve: #cba6f7;
        \\      --ctp-red: #f38ba8;
        \\      --ctp-maroon: #eba0ac;
        \\      --ctp-peach: #fab387;
        \\      --ctp-yellow: #f9e2af;
        \\      --ctp-green: #a6e3a1;
        \\      --ctp-teal: #94e2d5;
        \\      --ctp-sky: #89dceb;
        \\      --ctp-sapphire: #74c7ec;
        \\      --ctp-blue: #89b4fa;
        \\      --ctp-lavender: #b4befe;
        \\      --ctp-text: #cdd6f4;
        \\      --ctp-subtext1: #bac2de;
        \\      --ctp-subtext0: #a6adc8;
        \\      --ctp-overlay2: #9399b2;
        \\      --ctp-overlay1: #7f849c;
        \\      --ctp-overlay0: #6c7086;
        \\      --ctp-surface2: #585b70;
        \\      --ctp-surface1: #45475a;
        \\      --ctp-surface0: #313244;
        \\      --ctp-base: #1e1e2e;
        \\      --ctp-mantle: #181825;
        \\      --ctp-crust: #11111b;
        \\      /* Site tokens mapped to Mocha (+extra dark) */
        \\      --bg: var(--ctp-crust);
        \\      --panel: var(--ctp-mantle);
        \\      --text: var(--ctp-text);
        \\      --muted: var(--ctp-subtext0);
        \\      --border: var(--ctp-overlay0);
        \\      --accent: var(--ctp-blue);
        \\      --accent-2: var(--ctp-sapphire);
        \\    }
        \\    html, body { margin: 0; padding: 0; background: var(--bg); color: var(--text); font-family: system-ui, -apple-system, "Segoe UI", Roboto, Ubuntu, Cantarell, "Noto Sans", sans-serif; }
        \\    a { color: var(--accent); text-decoration: none; }
        \\    a:hover { color: var(--ctp-sky); text-decoration: underline; }
        \\    .suse { font-family: "SUSE", sans-serif; }
        \\    .mono { font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace; letter-spacing: .02em; }
        \\    .title { margin: 0 0 8px; font-weight: 700; }
        \\    .app { display: flex; gap: 16px; padding: 16px; box-sizing: border-box; height: 100vh; }
        \\    .pane { background: var(--panel); border: 1px solid var(--border); border-radius: 4px; display: flex; flex-direction: column; padding: 0; overflow: hidden; }
        \\    .status-bar { background: var(--ctp-mantle); }
        \\    #file-tree.pane { flex: 0 0 20rem; max-width: 20rem; }
        \\    #table-container.pane { flex: 1 1 auto; }
        \\    #file-tree-list.htmx-request, #table-container.htmx-request { opacity: 0.6; transition: opacity 200ms linear; }
        \\    .nav-list { list-style: none; margin: 0; padding: 0; }
        \\    .nav-item { border-bottom: 1px solid var(--border); }
        \\    .nav-item a { display: block; padding: 6px 8px; color: var(--text); }
        \\    .nav-item a:hover { background: var(--ctp-surface0); color: var(--accent); }
        \\    .pane-body { flex: 1 1 auto; min-height: 0; overflow-y: auto; overflow-x: auto; padding: 12px 0 12px 12px; scrollbar-gutter: stable; }
        \\    #table-container .pane-body { background: var(--ctp-surface0); }
        \\    .status-bar { display: flex; justify-content: space-between; align-items: center; gap: 12px; padding: 8px 12px; border-top: 1px solid var(--border); color: var(--muted); }
        \\    .table-wrap { overflow: visible; min-width: 100%; }
        \\    /* Visible scrollbars (Firefox + WebKit) */
        \\    .pane-body { scrollbar-width: auto; scrollbar-color: var(--ctp-overlay1) var(--ctp-surface1); }
        \\    .pane-body::-webkit-scrollbar { width: 12px; height: 10px; }
        \\    .pane-body::-webkit-scrollbar-track { background: var(--ctp-surface1); }
        \\    .pane-body::-webkit-scrollbar-thumb { background: var(--ctp-overlay1); border: 2px solid var(--ctp-surface1); border-radius: 4px; }
        \\    .pane-body::-webkit-scrollbar-thumb:hover { background: var(--ctp-overlay2); }
        \\    .table { width: max-content; min-width: 100%; border-collapse: collapse; font-size: 14px; table-layout: auto; background: var(--panel); color: var(--text); border-radius: 4px; }
        \\    .table thead th { position: sticky; top: 0; background: var(--ctp-surface0); text-align: left; padding: 8px; border-bottom: 1px solid var(--border); color: var(--ctp-subtext1); }
        \\    .table tbody td { padding: 6px 8px; border-bottom: 1px solid var(--border); vertical-align: top; }
        \\    .table tr:hover td { background: var(--ctp-surface0); }
        \\    a.mono { color: var(--accent-2); }
        \\    /* Sticky right columns for str_idx and iidx */
        \\    :root { --right-col-width: 7ch; --right-col-2-width: 16ch; }
        \\    .table { position: relative; }
        \\    .sticky-right-2 { position: sticky; right: var(--right-col-width); background: var(--ctp-surface0); z-index: 8; min-width: var(--right-col-2-width); max-width: var(--right-col-2-width); overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
        \\    .sticky-right { position: sticky; right: 0; background: var(--ctp-surface0); z-index: 9; min-width: var(--right-col-width); max-width: var(--right-col-width); text-align: right; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
        \\    tbody td.sticky-right, tbody td.sticky-right-2 { background: var(--panel); }
        \\    thead th.sticky-right, thead th.sticky-right-2 { box-shadow: -1px 0 0 0 var(--border) inset; z-index: 10; }
        \\  </style>
        \\</head>
        \\
        \\<body>
        \\  <div class="app">
        \\    <aside id="file-tree" class="pane">
        \\      <div class="pane-body">
        \\        <div id="file-tree-list" class="suse">
    ;

    pub const after_before_htmx =
        \\        </div>
        \\      </div>
        \\      <div class="status-bar"><span class="suse">Ultar Index Viewer</span></div>
        \\    </aside>
        \\
        \\    <main id="table-container" class="pane">
        \\    </main>
        \\  </div>
        \\  <script>
    ;

    pub const after_after_htmx =
        \\</script>
        \\</body>
        \\</html>
    ;

    pub const nav_list_open = "<ul class=\"nav-list\">";
    pub const nav_list_close = "</ul>";
    pub const table_wrap_open = "<div class=\"table-wrap\">";
    pub const table_open = "<table class=\"table\"><thead><tr>";
    pub const table_head_close_body_open = "</tr></thead><tbody>";
};

// Global gpa
var gpa_instance = std.heap.GeneralPurposeAllocator(.{}){};
const gpa = gpa_instance.allocator();

// Base directory to browse
var base_dir_buf: [std.fs.MAX_PATH_BYTES]u8 = undefined;
var base_dir: []const u8 = undefined;

fn trimTrailingSep(path: []const u8) []const u8 {
    var end: usize = path.len;
    while (end > 1 and path[end - 1] == std.fs.path.sep) {
        end -= 1;
    }
    return path[0..end];
}

// Join and normalize a path under base_dir; reject traversal outside base_dir
fn buildSafePathAlloc(alloc: std.mem.Allocator, rel: []const u8) ![]u8 {
    if (rel.len == 0) return error.InvalidPath;
    // Consider all incoming paths relative to base_dir, regardless of absolute/relative form
    const parts = &[_][]const u8{ base_dir, rel };
    const resolved = try std.fs.path.resolve(alloc, parts);
    return resolved; // allocated with alloc; caller frees (arena in route handlers)
}

fn urlEncodeAlloc(alloc: std.mem.Allocator, input: []const u8) ![]u8 {
    var out = try std.ArrayList(u8).initCapacity(alloc, input.len * 3);
    errdefer out.deinit(alloc);
    for (input) |b| {
        const is_unreserved = (b >= 'A' and b <= 'Z') or (b >= 'a' and b <= 'z') or (b >= '0' and b <= '9') or b == '-' or b == '_' or b == '.' or b == '~';
        if (is_unreserved) {
            try out.append(alloc, b);
        } else {
            try out.append(alloc, '%');
            const hex = "0123456789ABCDEF";
            try out.append(alloc, hex[b >> 4]);
            try out.append(alloc, hex[b & 0x0F]);
        }
    }
    return out.toOwnedSlice(alloc);
}

// Deprecated: rely on zap's getParamStr for decoded params.

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

// Initialize base directory
fn initBaseDir() !void {
    // Prefer DATA_PATH env var; fallback to current working directory
    if (std.posix.getenv("DATA_PATH")) |env_path| {
        // Resolve to absolute path if possible
        const env_slice: []const u8 = std.mem.sliceTo(env_path, 0);
        const abs = if (std.fs.path.isAbsolute(env_slice))
            env_slice
        else blk: {
            var arena = std.heap.ArenaAllocator.init(gpa);
            defer arena.deinit();
            const abs_join = try std.fs.path.resolve(arena.allocator(), &[_][]const u8{ ".", env_slice });
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

pub fn onRequest(r: zap.Request) anyerror!void {
    // Handle different routes
    if (r.path) |path| {
        if (std.mem.eql(u8, path, "/")) {
            handleIndex(r) catch |err| {
                std.debug.print("Error handling index: {}\n", .{err});
                if (@errorReturnTrace()) |trace| {
                    std.debug.dumpStackTrace(trace.*);
                }
                r.sendBody("Internal Server Error") catch {};
            };
        } else if (std.mem.startsWith(u8, path, "/browse")) {
            handleBrowse(r) catch |err| {
                std.debug.print("Error handling browse: {}\n", .{err});
                if (@errorReturnTrace()) |trace| {
                    std.debug.dumpStackTrace(trace.*);
                }
                r.sendBody("Internal Server Error") catch {};
            };
        } else if (std.mem.startsWith(u8, path, "/load")) {
            handleLoad(r) catch |err| {
                std.debug.print("Error handling load: {}\n", .{err});
                if (@errorReturnTrace()) |trace| {
                    std.debug.dumpStackTrace(trace.*);
                }
                r.sendBody("Internal Server Error") catch {};
            };
        } else if (std.mem.startsWith(u8, path, "/map_file")) {
            handleMapFile(r) catch |err| {
                std.debug.print("Error handling map_file: {}\n", .{err});
                if (@errorReturnTrace()) |trace| {
                    std.debug.dumpStackTrace(trace.*);
                }
                r.sendBody("Internal Server Error") catch {};
            };
        } else if (std.mem.startsWith(u8, path, "/boxed_file")) {
            handleBoxedFile(r) catch |err| {
                std.debug.print("Error handling boxed_file: {}\n", .{err});
                if (@errorReturnTrace()) |trace| {
                    std.debug.dumpStackTrace(trace.*);
                }
                r.sendBody("Internal Server Error") catch {};
            };
        } else if (std.mem.startsWith(u8, path, "/assets/preview/json/setup")) {
            handleJsonPreviewSetup(r) catch |err| {
                std.debug.print("Error handling json setup: {}\n", .{err});
                if (@errorReturnTrace()) |trace| {
                    std.debug.dumpStackTrace(trace.*);
                }
                r.sendBody("") catch {};
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

fn handleIndex(r: zap.Request) !void {
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const alloc = arena.allocator();

    r.parseQuery();

    // Always render with an empty list and trigger browse after load
    const initial_dir = r.getParamStr(alloc, "dir") catch null orelse "";
    const initial_file = r.getParamStr(alloc, "file") catch null orelse "";
    const enc_initial_dir = try urlEncodeAlloc(alloc, initial_dir);
    defer alloc.free(enc_initial_dir);
    var loader = try std.fmt.allocPrint(alloc, "<ul class=\"nav-list\" hx-get=\"/browse?dir={s}\" hx-trigger=\"load\" hx-target=\"#file-tree-list\" hx-swap=\"innerHTML\"></ul>", .{enc_initial_dir});
    defer alloc.free(loader);
    if (initial_file.len > 5 and std.mem.endsWith(u8, initial_file, ".utix")) {
        const enc_initial_file = try urlEncodeAlloc(alloc, initial_file);
        defer alloc.free(enc_initial_file);
        const table_loader = try std.fmt.allocPrint(alloc, "<div style=\"display:none\" hx-get=\"/load?file={s}\" hx-trigger=\"load\" hx-target=\"#table-container\" hx-swap=\"innerHTML\"></div>", .{enc_initial_file});
        defer alloc.free(table_loader);
        const combined = try std.fmt.allocPrint(alloc, "{s}{s}", .{ loader, table_loader });
        loader = combined;
    }

    const full_html = try renderBody(alloc, loader);
    try r.sendBody(full_html);
}

fn handleBrowse(r: zap.Request) !void {
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const alloc = arena.allocator();

    r.parseQuery();

    // Parse directory parameter
    const dir_param = r.getParamStr(alloc, "dir") catch null orelse "";

    std.debug.print("handleBrowse dir_param: {s}\n", .{dir_param});

    const entries = try listDirectory(alloc, dir_param);

    var html = try std.ArrayList(u8).initCapacity(alloc, 0);
    defer html.deinit(alloc);

    try html.appendSlice(alloc, Html.nav_list_open);

    // Add parent directory link if not at root
    if (dir_param.len > 0) {
        const parent = std.fs.path.dirname(dir_param) orelse "";
        try html.writer(alloc).print("<li class=\"nav-item\"><a href=\"/?dir={s}\" hx-get=\"/browse?dir={s}\" hx-target=\"#file-tree-list\" hx-swap=\"innerHTML\" hx-push-url=\"/?dir={s}\">⬆️ ..</a></li>", .{ parent, parent, parent });
    }

    for (entries) |entry| {
        const icon = if (entry.typ == .dir) "📁" else "📄";
        const endpoint = if (entry.typ == .dir) "browse" else "load";
        const target = if (entry.typ == .dir) "#file-tree-list" else "#table-container";

        try html.appendSlice(alloc, "<li class=\"nav-item\">");
        const enc_rel = try urlEncodeAlloc(alloc, entry.rel_path);
        defer alloc.free(enc_rel);
        const enc_dir = try urlEncodeAlloc(alloc, dir_param);
        defer alloc.free(enc_dir);
        const push_url = if (entry.typ == .dir)
            try std.fmt.allocPrint(alloc, "/?dir={s}", .{enc_rel})
        else
            try std.fmt.allocPrint(alloc, "/?dir={s}&file={s}", .{ enc_dir, enc_rel });
        defer alloc.free(push_url);

        try html.writer(alloc).print("<a href=\"/{s}?{s}\" hx-get=\"/{s}?{s}\" hx-target=\"{s}\" hx-indicator=\"#file-tree-list\" hx-swap=\"innerHTML\" hx-push-url=\"{s}\">{s} {s}</a></li>", .{
            endpoint,
            if (entry.typ == .dir)
                (try std.fmt.allocPrint(alloc, "dir={s}", .{enc_rel}))
            else
                (try std.fmt.allocPrint(alloc, "file={s}", .{enc_rel})),
            endpoint,
            if (entry.typ == .dir)
                (try std.fmt.allocPrint(alloc, "dir={s}", .{enc_rel}))
            else
                (try std.fmt.allocPrint(alloc, "file={s}", .{enc_rel})),
            target,
            push_url,
            icon,
            entry.name,
        });
    }
    try html.appendSlice(alloc, Html.nav_list_close);

    try r.sendBody(html.items);
}

fn handleJsonPreviewSetup(r: zap.Request) !void {
    // Inject once: lightweight runtime that fetches data-src from .json-code,
    // pretty-prints, and highlights using highlight.js loaded from CDN.
    const snippet =
        \\<link id="hljs-css" rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/highlight.js/11.11.1/styles/github-dark.min.css">
        \\<script id="hljs-js" src="https://cdnjs.cloudflare.com/ajax/libs/highlight.js/11.11.1/highlight.min.js"></script>
        \\<script id="ultar-json-rt">(function(){
        \\  async function ensure(){
        \\    if(!window.hljs){
        \\      await new Promise(r=>{ var s=document.getElementById('hljs-js'); if(s && !s.dataset._loaded){ s.addEventListener('load', ()=>{s.dataset._loaded='1'; r();}, {once:true}); } else { r(); } });
        \\    }
        \\  }
        \\  async function init(el){
        \\    const url = el.getAttribute('data-src');
        \\    if(!url) return;
        \\    const res = await fetch(url);
        \\    const txt = await res.text();
        \\    let out = txt;
        \\    try { out = JSON.stringify(JSON.parse(txt), null, 2); } catch(e){}
        \\    el.textContent = out;
        \\    await ensure();
        \\    if(window.hljs){ window.hljs.highlightElement(el); }
        \\  }
        \\  function scan(){ document.querySelectorAll('.json-code:not([data-initialized])').forEach(function(el){ el.setAttribute('data-initialized','1'); init(el); }); }
        \\  document.body.addEventListener('htmx:afterSwap', scan);
        \\  scan();
        \\})();</script>
    ;
    r.setHeader("Content-Type", "text/html; charset=utf-8") catch {};
    try r.sendBody(snippet);
}

// Render body content with base template
fn renderBody(alloc: std.mem.Allocator, body_html: []const u8) ![]const u8 {
    var result = try std.ArrayList(u8).initCapacity(alloc, 0);
    try result.appendSlice(alloc, Html.before_body);
    try result.appendSlice(alloc, body_html);
    try result.appendSlice(alloc, Html.after_before_htmx);
    try result.appendSlice(alloc, htmx_embed.htmx_js);
    try result.appendSlice(alloc, Html.after_after_htmx);
    return result.toOwnedSlice(alloc);
}

fn handleLoad(r: zap.Request) !void {
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const alloc = arena.allocator();

    r.parseQuery();

    // Parse query parameter: support new 'file' and legacy 'path'
    const path_param = r.getParamStr(alloc, "file") catch (r.getParamStr(alloc, "path") catch null) orelse "";

    if (path_param.len == 0) {
        try r.sendBody("<p>No path specified</p>");
        return;
    }

    // Build full path to .utix file
    const full_path = try buildSafePathAlloc(alloc, path_param);
    defer alloc.free(full_path);

    // Check if file exists
    std.fs.accessAbsolute(full_path, .{}) catch {
        try r.sendBody("<p>File not found</p>");
        return;
    };

    // Parse the .utix file
    const entries = parseUtixFile(alloc, full_path) catch {
        std.debug.print("Error parsing .utix file\n", .{});
        try r.sendBody("<p>Error parsing index file</p>");
        return;
    };
    defer alloc.free(entries);

    if (entries.len == 0) {
        try r.sendBody("<p>No items found in the index file.</p>");
        return;
    }

    // Generate HTML table within a scrolling pane body and a bottom status bar
    var html = try std.ArrayList(u8).initCapacity(alloc, 0);
    defer html.deinit(alloc);

    // Get tar file path (remove .utix extension)
    const tar_path = if (std.mem.endsWith(u8, path_param, ".utix"))
        path_param[0 .. path_param.len - 5]
    else
        path_param;

    try html.appendSlice(alloc, "<div class=\"pane-body\">");
    try html.appendSlice(alloc, Html.table_wrap_open);
    try html.appendSlice(alloc, Html.table_open);

    // Compute union of keys across entries
    var key_set = std.StringHashMap(void).init(alloc);
    defer key_set.deinit();
    for (entries) |e| {
        for (e.keys) |k| {
            if (!key_set.contains(k)) try key_set.put(k, {});
        }
    }

    // Build a stable ordered list of keys (sorted)
    var all_keys_list = try std.ArrayList([]const u8).initCapacity(alloc, key_set.count());
    defer all_keys_list.deinit(alloc);
    var it = key_set.iterator();
    while (it.next()) |kv| {
        try all_keys_list.append(alloc, kv.key_ptr.*);
    }
    std.mem.sort([]const u8, all_keys_list.items, {}, struct {
        fn lessThan(_: void, a: []const u8, b: []const u8) bool {
            return std.mem.lessThan(u8, a, b);
        }
    }.lessThan);

    // Render headers for union keys
    for (all_keys_list.items) |k| {
        try html.writer(alloc).print("<th class=\"suse\">{s}</th>", .{k});
    }
    // Pinned right headers: str_idx and iidx
    try html.appendSlice(alloc, "<th class=\"sticky-right-2 suse\">id</th>");
    try html.appendSlice(alloc, "<th class=\"sticky-right suse\">row</th>");
    try html.appendSlice(alloc, Html.table_head_close_body_open);

    // Generate table rows
    for (entries, 0..) |entry, idx| {
        try html.writer(alloc).print("<tr data-index=\"{d}\">", .{idx});

        // For quick lookup: map key -> index within this entry
        var per_row = std.StringHashMap(usize).init(alloc);
        defer per_row.deinit();
        for (entry.keys, 0..) |k, kidx| {
            try per_row.put(k, kidx);
        }

        for (all_keys_list.items) |k| {
            if (per_row.get(k)) |kidx| {
                if (kidx < entry.offsets.len and kidx < entry.sizes.len) {
                    const offset = entry.offset + entry.offsets[kidx];
                    const size = entry.sizes[kidx];
                    const range_str = try std.fmt.allocPrint(alloc, "{X:0>8}..{X:0>8}", .{ offset, offset + size });
                    defer alloc.free(range_str);
                    const id_value = if (entry.str_idx.len > 0) entry.str_idx else blk: {
                        const tmp = try std.fmt.allocPrint(alloc, "{X}", .{entry.iidx});
                        break :blk tmp;
                    };
                    defer if (id_value.ptr != entry.str_idx.ptr) alloc.free(id_value);
                    const enc_file = try urlEncodeAlloc(alloc, tar_path);
                    defer alloc.free(enc_file);
                    const enc_k = try urlEncodeAlloc(alloc, k);
                    defer alloc.free(enc_k);
                    const enc_id = try urlEncodeAlloc(alloc, id_value);
                    defer alloc.free(enc_id);
                    const link = try std.fmt.allocPrint(alloc, "/boxed_file?file={s}&k={s}&base={x}&end={x}&id={s}", .{ enc_file, enc_k, offset, offset + size, enc_id });
                    defer alloc.free(link);
                    try html.writer(alloc).print("<td><a class=\"mono\" hx-get=\"{s}\" hx-swap=\"outerHTML\">{s}</a></td>", .{ link, range_str });
                } else {
                    try html.appendSlice(alloc, "<td></td>");
                }
            } else {
                try html.appendSlice(alloc, "<td></td>");
            }
        }
        // Pinned right cells
        const id_value_row = if (entry.str_idx.len > 0) entry.str_idx else blk: {
            const tmp = try std.fmt.allocPrint(alloc, "{X}", .{entry.iidx});
            break :blk tmp;
        };
        defer if (id_value_row.ptr != entry.str_idx.ptr) alloc.free(id_value_row);
        const row_str = try std.fmt.allocPrint(alloc, "{d}", .{entry.iidx});
        defer alloc.free(row_str);
        const row_trim = if (row_str.len > 5) row_str[row_str.len - 5 ..] else row_str;
        try html.writer(alloc).print("<td class=\"sticky-right-2 mono\">{s}</td>", .{id_value_row});
        try html.writer(alloc).print("<td class=\"sticky-right mono\">{s}</td>", .{row_trim});

        try html.appendSlice(alloc, "</tr>");
    }

    try html.appendSlice(alloc, "</tbody></table></div></div>");

    // Bottom status bar with the utix/tar path
    try html.writer(alloc).print("<div class=\"status-bar\"><span class=\"suse\">{s}</span></div>", .{tar_path});

    try r.sendBody(html.items);
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

fn handleMapFile(r: zap.Request) !void {
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const alloc = arena.allocator();

    r.parseQuery();

    // Parse query parameters (decoded via zap)
    const file_param = r.getParamStr(alloc, "file") catch null orelse "";
    const base_param = r.getParamStr(alloc, "base") catch null orelse "";
    const end_param = r.getParamStr(alloc, "end") catch null orelse "";
    const k_param = r.getParamStr(alloc, "k") catch null orelse "";
    const id_param = r.getParamStr(alloc, "id") catch null;

    if (file_param.len == 0 or base_param.len == 0 or end_param.len == 0 or k_param.len == 0) {
        r.setStatus(.bad_request);
        try r.sendBody("Missing required parameters");
        return;
    }

    // Parse hex values
    const base_offset = std.fmt.parseInt(u64, base_param, 16) catch {
        r.setStatus(.bad_request);
        try r.sendBody("Invalid base offset");
        return;
    };

    const end_offset = std.fmt.parseInt(u64, end_param, 16) catch {
        r.setStatus(.bad_request);
        try r.sendBody("Invalid end offset");
        return;
    };

    if (end_offset <= base_offset) {
        r.setStatus(.bad_request);
        try r.sendBody("Invalid offset range");
        return;
    }

    // Build full file path
    const full_path = try buildSafePathAlloc(alloc, file_param);
    defer alloc.free(full_path);

    // Open and memory map the file
    var file = std.fs.openFileAbsolute(full_path, .{}) catch {
        r.setStatus(.not_found);
        try r.sendBody("File not found");
        return;
    };
    defer file.close();

    const file_size = try file.getEndPos();
    if (end_offset > file_size) {
        r.setStatus(.bad_request);
        try r.sendBody("End offset exceeds file size");
        return;
    }

    // Read the data
    const data_size = end_offset - base_offset;
    const data = try alloc.alloc(u8, data_size);
    defer alloc.free(data);

    _ = try file.seekTo(base_offset);
    const bytes_read = try file.read(data);
    if (bytes_read != data_size) {
        r.setStatus(.internal_server_error);
        try r.sendBody("Failed to read file data");
        return;
    }

    // Set appropriate MIME type
    const mime_type = getMimeType(k_param);
    if (mime_type) |mt| {
        r.setHeader("Content-Type", mt) catch {};
    }

    // Content-Disposition filename: {str_idx}.{key}
    if (id_param) |id_val| {
        const cleaned_id = try sanitizeFilename(alloc, id_val);
        defer alloc.free(cleaned_id);
        var filename: []const u8 = undefined;
        if (k_param.len > 0 and k_param[0] == '.') {
            filename = try std.fmt.allocPrint(alloc, "{s}{s}", .{ cleaned_id, k_param });
        } else if (k_param.len > 0) {
            filename = try std.fmt.allocPrint(alloc, "{s}.{s}", .{ cleaned_id, k_param });
        } else {
            filename = try std.fmt.allocPrint(alloc, "{s}", .{cleaned_id});
        }
        defer alloc.free(filename);
        const header_val = try std.fmt.allocPrint(alloc, "attachment; filename=\"{s}\"", .{filename});
        defer alloc.free(header_val);
        r.setHeader("Content-Disposition", header_val) catch {};
    }

    // Send the data
    r.setStatus(.ok);
    try r.sendBody(data);
}

// Get MIME type for file extension
fn getMimeType(filename: []const u8) ?[]const u8 {
    if (std.mem.endsWith(u8, filename, ".jpg") or std.mem.endsWith(u8, filename, ".jpeg")) {
        return "image/jpeg";
    } else if (std.mem.endsWith(u8, filename, ".png")) {
        return "image/png";
    } else if (std.mem.endsWith(u8, filename, ".gif")) {
        return "image/gif";
    } else if (std.mem.endsWith(u8, filename, ".webp")) {
        return "image/webp";
    } else if (std.mem.endsWith(u8, filename, ".svg")) {
        return "image/svg+xml";
    } else if (std.mem.endsWith(u8, filename, ".txt")) {
        return "text/plain";
    } else if (std.mem.endsWith(u8, filename, ".html") or std.mem.endsWith(u8, filename, ".htm")) {
        return "text/html";
    } else if (std.mem.endsWith(u8, filename, ".json")) {
        return "application/json";
    } else if (std.mem.endsWith(u8, filename, ".xml")) {
        return "application/xml";
    } else if (std.mem.endsWith(u8, filename, ".pdf")) {
        return "application/pdf";
    } else if (std.mem.endsWith(u8, filename, ".zip")) {
        return "application/zip";
    } else if (std.mem.endsWith(u8, filename, ".tar")) {
        return "application/x-tar";
    } else {
        return null;
    }
}

fn handleBoxedFile(r: zap.Request) !void {
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const alloc = arena.allocator();

    r.parseQuery();

    // Parse query parameters
    const file_param = r.getParamStr(alloc, "file") catch null orelse "";
    const base_param = r.getParamStr(alloc, "base") catch null orelse "";
    const end_param = r.getParamStr(alloc, "end") catch null orelse "";
    const k_param = r.getParamStr(alloc, "k") catch null orelse "";
    const id_param = r.getParamStr(alloc, "id") catch null orelse "";

    if (file_param.len == 0 or base_param.len == 0 or end_param.len == 0 or k_param.len == 0) {
        r.setStatus(.bad_request);
        try r.sendBody("<p>Missing parameters</p>");
        return;
    }

    // Validate offsets
    _ = std.fmt.parseInt(u64, base_param, 16) catch {
        r.setStatus(.bad_request);
        try r.sendBody("<p>Invalid base offset</p>");
        return;
    };
    const end_offset = std.fmt.parseInt(u64, end_param, 16) catch {
        r.setStatus(.bad_request);
        try r.sendBody("<p>Invalid end offset</p>");
        return;
    };
    _ = end_offset; // only format validation here; actual safety in map_file

    // Build download URL to the map_file endpoint (include id for naming)
    const enc_file = try urlEncodeAlloc(alloc, file_param);
    defer alloc.free(enc_file);
    const enc_k = try urlEncodeAlloc(alloc, k_param);
    defer alloc.free(enc_k);
    const enc_base = try urlEncodeAlloc(alloc, base_param);
    defer alloc.free(enc_base);
    const enc_end = try urlEncodeAlloc(alloc, end_param);
    defer alloc.free(enc_end);
    const enc_id = try urlEncodeAlloc(alloc, id_param);
    defer alloc.free(enc_id);
    const download_url = try std.fmt.allocPrint(
        alloc,
        "/map_file?file={s}&k={s}&base={s}&end={s}&id={s}",
        .{ enc_file, enc_k, enc_base, enc_end, enc_id },
    );
    defer alloc.free(download_url);

    // Decide preview based on MIME type
    const mt = getMimeType(k_param);

    var html = try std.ArrayList(u8).initCapacity(alloc, 0);
    defer html.deinit(alloc);

    // Always include a download link
    try html.writer(alloc).print(
        "<div class=\"boxed-preview\"><div><a class=\"suse\" href=\"{s}\" download>download</a></div>",
        .{download_url},
    );

    const mt_str = mt orelse "";
    std.debug.print("key={s} mimeType: {s}\n", .{ k_param, mt_str });

    if (mt) |mime_type| {
        if (std.mem.startsWith(u8, mime_type, "image/")) {
            const snip = try preview_image.render(alloc, download_url);
            defer alloc.free(snip);
            try html.appendSlice(alloc, snip);
        } else if (std.mem.eql(u8, mime_type, "application/json")) {
            const snip = try preview_json.render(alloc, download_url);
            defer alloc.free(snip);
            try html.appendSlice(alloc, snip);
        } else if (std.mem.startsWith(u8, mime_type, "video/")) {
            try html.writer(alloc).print(
                "<div style=\"margin-top:6px\"><video controls src=\"{s}\" style=\"max-width:512px; max-height:384px;\"></video></div>",
                .{download_url},
            );
        } else if (std.mem.startsWith(u8, mime_type, "audio/")) {
            try html.writer(alloc).print(
                "<div style=\"margin-top:6px\"><audio controls src=\"{s}\"></audio></div>",
                .{download_url},
            );
        } else if (std.mem.startsWith(u8, mime_type, "text/")) {
            try html.writer(alloc).print(
                "<div style=\"margin-top:6px\"><iframe src=\"{s}\" style=\"width:100%; max-width:720px; height:360px; background:var(--ctp-crust); border:1px solid var(--border);\"></iframe></div>",
                .{download_url},
            );
        } else {
            // Fallback embed
            try html.writer(alloc).print(
                "<div style=\"margin-top:6px\"><embed src=\"{s}\" type=\"{s}\" style=\"max-width:720px; height:360px; border:1px solid var(--border);\"/></div>",
                .{ download_url, mime_type },
            );
        }
    }

    try html.appendSlice(alloc, "</div>");
    try r.sendBody(html.items);
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
