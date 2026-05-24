//! Zig wrapper around the `mustach` C library. Renders typed data structs to a writer.

const std = @import("std");

const c = @cImport({
    @cInclude("mustach.h");
});

// Field names are part of the template contract; do not rename without updating templates.

pub const FileListItem = struct {
    is_dir: bool,
    is_unindexed_tar: bool,
    name: []const u8,
    rel_path_enc: []const u8,
    dir_enc: []const u8,
};

pub const FileListData = struct {
    show_parent: bool,
    parent: []const u8,
    entries: []const FileListItem,
};

pub const LoadTableHeader = struct {
    name: []const u8,
    enc_name: []const u8,
};

pub const LoadTableCell = struct {
    has: bool,
    /// Pre-formatted `"{X:0>8}..{X:0>8}"`.
    range_str: []const u8,
};

pub const LoadTableRow = struct {
    cells: []const LoadTableCell,
    id_value: []const u8,
    row_idx: i64,
    has_metadata: bool = false,
    metadata_json: ?[]const u8 = null,
};

pub const LoadTablePageLink = struct {
    num: []const u8,
    url: []const u8,
    is_current: bool,
};

pub const LoadTableData = struct {
    enc_file: []const u8,
    headers: []const LoadTableHeader,
    rows: []const LoadTableRow,
    tar_path: []const u8,
    /// Decimal-formatted string.
    total_rows: []const u8,
    has_pages: bool,
    has_prev: bool,
    has_next: bool,
    prev_url: []const u8,
    next_url: []const u8,
    pages: []const LoadTablePageLink,
};

pub const BaseData = struct {
    body: []const u8,
    table_body: []const u8,
};

pub const RenderError = error{
    MustachRenderFailed,
} || std.Io.Writer.Error;

const RootKind = enum { base, file_list, load_table };

const FrameTag = enum {
    base_root,
    file_list_root,
    file_list_entry,
    load_table_root,
    load_table_header,
    load_table_row,
    load_table_cell,
    load_table_page,
    /// Truthy `{{#bool}}` section; name resolution falls through to the frame below.
    bool_section,
};

const Frame = struct {
    tag: FrameTag,
    idx: usize = 0,
    len: usize = 0,
};

const MAX_DEPTH: usize = 16;

/// Fixed-capacity stack of section frames owned by `MustachContext`.
const FrameStack = struct {
    buf: [MAX_DEPTH]Frame = undefined,
    len: usize = 0,

    fn push(self: *FrameStack, frame: Frame) error{Overflow}!void {
        if (self.len >= MAX_DEPTH) return error.Overflow;
        self.buf[self.len] = frame;
        self.len += 1;
    }

    fn pop(self: *FrameStack) void {
        if (self.len > 0) self.len -= 1;
    }

    fn slice(self: *FrameStack) []Frame {
        return self.buf[0..self.len];
    }
};

const MustachContext = struct {
    kind: RootKind,
    base: ?*const BaseData = null,
    file_list: ?*const FileListData = null,
    load_table: ?*const LoadTableData = null,
    frames: FrameStack = .{},

    fn topFrame(self: *MustachContext) ?*Frame {
        const items = self.frames.slice();
        if (items.len == 0) return null;
        return &items[items.len - 1];
    }

    /// Nearest non-`bool_section` frame, so bool sections inherit the enclosing scope.
    fn dataFrame(self: *MustachContext) ?*Frame {
        const items = self.frames.slice();
        var i: usize = items.len;
        while (i > 0) {
            i -= 1;
            if (items[i].tag != .bool_section) return &items[i];
        }
        return null;
    }
};

fn nameSlice(name: [*c]const u8) []const u8 {
    if (name == null) return &[_]u8{};
    return std.mem.span(@as([*:0]const u8, @ptrCast(name)));
}

fn writeSbuf(sbuf: [*c]c.struct_mustach_sbuf, value: []const u8) void {
    // Zero first so the anonymous freecb/releasecb union doesn't depend on
    // @cImport's generated field name.
    sbuf.* = std.mem.zeroes(c.struct_mustach_sbuf);
    if (value.len > 0) sbuf.*.value = @ptrCast(value.ptr);
    sbuf.*.length = value.len;
}

fn cbEnter(closure: ?*anyopaque, name: [*c]const u8) callconv(.c) c_int {
    const ctx: *MustachContext = @ptrCast(@alignCast(closure orelse return 0));
    const key = nameSlice(name);
    return enterImpl(ctx, key);
}

fn cbNext(closure: ?*anyopaque) callconv(.c) c_int {
    const ctx: *MustachContext = @ptrCast(@alignCast(closure orelse return 0));
    const top = ctx.topFrame() orelse return 0;
    switch (top.tag) {
        .file_list_entry,
        .load_table_header,
        .load_table_row,
        .load_table_cell,
        .load_table_page,
        => {
            top.idx += 1;
            return if (top.idx < top.len) 1 else 0;
        },
        else => return 0,
    }
}

fn cbLeave(closure: ?*anyopaque) callconv(.c) c_int {
    const ctx: *MustachContext = @ptrCast(@alignCast(closure orelse return 0));
    if (ctx.frames.len == 0) return c.MUSTACH_ERROR_CLOSING;
    ctx.frames.pop();
    return c.MUSTACH_OK;
}

fn cbGet(closure: ?*anyopaque, name: [*c]const u8, sbuf: [*c]c.struct_mustach_sbuf) callconv(.c) c_int {
    const ctx: *MustachContext = @ptrCast(@alignCast(closure orelse return c.MUSTACH_ERROR_ITEM_NOT_FOUND));
    const key = nameSlice(name);
    const value = resolveScalar(ctx, key) orelse "";
    writeSbuf(sbuf, value);
    return c.MUSTACH_OK;
}

/// Returns 1 if a frame was pushed (descend), 0 otherwise.
fn enterImpl(ctx: *MustachContext, key: []const u8) c_int {
    const data_frame = ctx.dataFrame() orelse return 0;

    switch (ctx.kind) {
        .base => return 0,
        .file_list => switch (data_frame.tag) {
            .file_list_root => {
                const fl = ctx.file_list orelse return 0;
                if (std.mem.eql(u8, key, "show_parent")) {
                    return pushBoolFrame(ctx, fl.show_parent);
                }
                if (std.mem.eql(u8, key, "entries")) {
                    return pushArrayFrame(ctx, .file_list_entry, fl.entries.len);
                }
                return 0;
            },
            .file_list_entry => {
                const fl = ctx.file_list orelse return 0;
                const idx = data_frame.idx;
                if (idx >= fl.entries.len) return 0;
                const e = fl.entries[idx];
                if (std.mem.eql(u8, key, "is_dir")) return pushBoolFrame(ctx, e.is_dir);
                if (std.mem.eql(u8, key, "is_unindexed_tar")) return pushBoolFrame(ctx, e.is_unindexed_tar);
                return 0;
            },
            else => return 0,
        },
        .load_table => switch (data_frame.tag) {
            .load_table_root => {
                const lt = ctx.load_table orelse return 0;
                if (std.mem.eql(u8, key, "has_pages")) return pushBoolFrame(ctx, lt.has_pages);
                if (std.mem.eql(u8, key, "has_prev")) return pushBoolFrame(ctx, lt.has_prev);
                if (std.mem.eql(u8, key, "has_next")) return pushBoolFrame(ctx, lt.has_next);
                if (std.mem.eql(u8, key, "headers")) return pushArrayFrame(ctx, .load_table_header, lt.headers.len);
                if (std.mem.eql(u8, key, "rows")) return pushArrayFrame(ctx, .load_table_row, lt.rows.len);
                if (std.mem.eql(u8, key, "pages")) return pushArrayFrame(ctx, .load_table_page, lt.pages.len);
                return 0;
            },
            .load_table_row => {
                const lt = ctx.load_table orelse return 0;
                if (data_frame.idx >= lt.rows.len) return 0;
                const row = lt.rows[data_frame.idx];
                if (std.mem.eql(u8, key, "cells")) return pushArrayFrame(ctx, .load_table_cell, row.cells.len);
                if (std.mem.eql(u8, key, "has_metadata")) return pushBoolFrame(ctx, row.has_metadata);
                return 0;
            },
            .load_table_cell => {
                const lt = ctx.load_table orelse return 0;
                // Walk up to the enclosing row frame to recover its iterator index.
                const items = ctx.frames.slice();
                var row_idx: ?usize = null;
                var i: usize = items.len;
                while (i > 0) {
                    i -= 1;
                    if (items[i].tag == .load_table_row) {
                        row_idx = items[i].idx;
                        break;
                    }
                }
                const r = row_idx orelse return 0;
                if (r >= lt.rows.len) return 0;
                if (data_frame.idx >= lt.rows[r].cells.len) return 0;
                const cell = lt.rows[r].cells[data_frame.idx];
                if (std.mem.eql(u8, key, "has")) return pushBoolFrame(ctx, cell.has);
                return 0;
            },
            .load_table_page => {
                const lt = ctx.load_table orelse return 0;
                if (data_frame.idx >= lt.pages.len) return 0;
                const p = lt.pages[data_frame.idx];
                if (std.mem.eql(u8, key, "is_current")) return pushBoolFrame(ctx, p.is_current);
                return 0;
            },
            else => return 0,
        },
    }
}

fn pushBoolFrame(ctx: *MustachContext, value: bool) c_int {
    if (!value) return 0;
    ctx.frames.push(.{ .tag = .bool_section }) catch return c.MUSTACH_ERROR_TOO_DEEP;
    return 1;
}

fn pushArrayFrame(ctx: *MustachContext, tag: FrameTag, len: usize) c_int {
    if (len == 0) return 0;
    ctx.frames.push(.{ .tag = tag, .idx = 0, .len = len }) catch return c.MUSTACH_ERROR_TOO_DEEP;
    return 1;
}

// Mustach copies the sbuf value before the next callback returns, so this
// buffer may be reused across calls on the same thread.
threadlocal var scratch_buf: [64]u8 = undefined;

fn fmtI64(value: i64) []const u8 {
    return std.fmt.bufPrint(&scratch_buf, "{d}", .{value}) catch "";
}

fn resolveScalar(ctx: *MustachContext, key: []const u8) ?[]const u8 {
    const data_frame = ctx.dataFrame() orelse return null;
    switch (ctx.kind) {
        .base => {
            const b = ctx.base orelse return null;
            if (std.mem.eql(u8, key, "body")) return b.body;
            if (std.mem.eql(u8, key, "table_body")) return b.table_body;
            return null;
        },
        .file_list => {
            const fl = ctx.file_list orelse return null;
            switch (data_frame.tag) {
                .file_list_root => {
                    if (std.mem.eql(u8, key, "parent")) return fl.parent;
                    return null;
                },
                .file_list_entry => {
                    if (data_frame.idx >= fl.entries.len) return null;
                    const e = fl.entries[data_frame.idx];
                    if (std.mem.eql(u8, key, "name")) return e.name;
                    if (std.mem.eql(u8, key, "rel_path_enc")) return e.rel_path_enc;
                    if (std.mem.eql(u8, key, "dir_enc")) return e.dir_enc;
                    return null;
                },
                else => return null,
            }
        },
        .load_table => {
            const lt = ctx.load_table orelse return null;
            switch (data_frame.tag) {
                .load_table_root => {
                    if (std.mem.eql(u8, key, "enc_file")) return lt.enc_file;
                    if (std.mem.eql(u8, key, "tar_path")) return lt.tar_path;
                    if (std.mem.eql(u8, key, "total_rows")) return lt.total_rows;
                    if (std.mem.eql(u8, key, "prev_url")) return lt.prev_url;
                    if (std.mem.eql(u8, key, "next_url")) return lt.next_url;
                    return null;
                },
                .load_table_header => {
                    if (data_frame.idx >= lt.headers.len) return null;
                    const h = lt.headers[data_frame.idx];
                    if (std.mem.eql(u8, key, "name")) return h.name;
                    if (std.mem.eql(u8, key, "enc_name")) return h.enc_name;
                    return null;
                },
                .load_table_row => {
                    if (data_frame.idx >= lt.rows.len) return null;
                    const r = lt.rows[data_frame.idx];
                    if (std.mem.eql(u8, key, "id_value")) return r.id_value;
                    if (std.mem.eql(u8, key, "row_idx")) return fmtI64(r.row_idx);
                    if (std.mem.eql(u8, key, "metadata_json")) return r.metadata_json orelse "";
                    return null;
                },
                .load_table_cell => {
                    // Walk up to the enclosing row frame to recover its iterator index.
                    const items = ctx.frames.slice();
                    var row_idx: ?usize = null;
                    var i: usize = items.len;
                    while (i > 0) {
                        i -= 1;
                        if (items[i].tag == .load_table_row) {
                            row_idx = items[i].idx;
                            break;
                        }
                    }
                    const r = row_idx orelse return null;
                    if (r >= lt.rows.len) return null;
                    if (data_frame.idx >= lt.rows[r].cells.len) return null;
                    const cell = lt.rows[r].cells[data_frame.idx];
                    if (std.mem.eql(u8, key, "range_str")) return cell.range_str;
                    return null;
                },
                .load_table_page => {
                    if (data_frame.idx >= lt.pages.len) return null;
                    const p = lt.pages[data_frame.idx];
                    if (std.mem.eql(u8, key, "num")) return p.num;
                    if (std.mem.eql(u8, key, "url")) return p.url;
                    return null;
                },
                else => return null,
            }
        },
    }
}

fn renderInner(template_bytes: []const u8, ctx: *MustachContext, w: *std.Io.Writer) RenderError!void {
    var itf: c.struct_mustach_itf = .{
        .start = null,
        .put = null,
        .enter = cbEnter,
        .next = cbNext,
        .leave = cbLeave,
        .partial = null,
        .emit = null,
        .get = cbGet,
        .stop = null,
    };

    var result_ptr: [*c]u8 = null;
    var result_len: usize = 0;
    const rc = c.mustach_mem(
        @ptrCast(template_bytes.ptr),
        template_bytes.len,
        &itf,
        @ptrCast(ctx),
        c.Mustach_With_AllExtensions,
        &result_ptr,
        &result_len,
    );

    // `mustach_mem` returns a malloc'd buffer; free with libc. `std.c.free(null)` is a no-op.
    defer std.c.free(@as(?*anyopaque, @ptrCast(result_ptr)));

    if (rc != c.MUSTACH_OK) return error.MustachRenderFailed;
    if (result_len == 0) return;
    try w.writeAll(result_ptr[0..result_len]);
}

// `arena` is reserved for API symmetry; current implementations don't allocate from it.

pub fn renderFileList(
    arena: std.mem.Allocator,
    template_bytes: []const u8,
    data: FileListData,
    w: *std.Io.Writer,
) RenderError!void {
    _ = arena;
    var ctx: MustachContext = .{ .kind = .file_list, .file_list = &data };
    ctx.frames.push(.{ .tag = .file_list_root }) catch return error.MustachRenderFailed;
    try renderInner(template_bytes, &ctx, w);
}

pub fn renderLoadTable(
    arena: std.mem.Allocator,
    template_bytes: []const u8,
    data: LoadTableData,
    w: *std.Io.Writer,
) RenderError!void {
    _ = arena;
    var ctx: MustachContext = .{ .kind = .load_table, .load_table = &data };
    ctx.frames.push(.{ .tag = .load_table_root }) catch return error.MustachRenderFailed;
    try renderInner(template_bytes, &ctx, w);
}

pub fn renderBase(
    arena: std.mem.Allocator,
    template_bytes: []const u8,
    data: BaseData,
    w: *std.Io.Writer,
) RenderError!void {
    _ = arena;
    var ctx: MustachContext = .{ .kind = .base, .base = &data };
    ctx.frames.push(.{ .tag = .base_root }) catch return error.MustachRenderFailed;
    try renderInner(template_bytes, &ctx, w);
}
