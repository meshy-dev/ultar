//! Python ABI3 bindings for ultar DataLoader.
//!
//! ## Ownership Layout
//!
//! ```
//! module object
//! `-- ModuleState
//!     |-- data_loader_type: ?*PyTypeObject
//!     `-- loaded_row_type: ?*PyTypeObject
//!
//! DataLoaderObject
//! |-- ob_base: PyObject          (refcount managed by Python)
//! |-- typ: ?*PyTypeObject        (cached heap type for dealloc)
//! `-- loader: ?*LuaLoaderCCtx    (native, destroyed once in dealloc)
//!
//! LoadedRowObject
//! |-- ob_base: PyObject          (refcount managed by Python)
//! |-- typ: ?*PyTypeObject        (cached heap type for dealloc)
//! |-- parent: ?*DataLoaderObject (incref'd reference to parent)
//! `-- row: ?*LoadedRow           (native ptr, reclaimed once before parent release)
//! ```
//!
//! ## Reference Ownership
//!
//! - `DataLoaderObject`: Created by `tp_new`, returned to Python with refcount=1.
//!   Caller owns it. The instance caches its heap type pointer so custom dealloc can
//!   call `tp_free` and discharge the heap-type reference without reading `PyObject`
//!   headers directly.
//!
//! - `LoadedRowObject`: Created by `dataLoaderNext`, returned with refcount=1.
//!   Holds an incref'd reference to its parent `DataLoaderObject` to keep it alive.
//!   It also caches its heap type pointer for the same dealloc rule. On dealloc,
//!   it reclaims the native row to the parent's loader, then decrefs parent.
//!
//! - No reference cycles: LoadedRow → DataLoader (one-way ownership).
//!
//! ## Error Handling Pattern
//!
//! Internal functions use Zig error semantics (`PyError!T`) with explicit partial-init
//! cleanup. Python-owned fields are zeroed before fallible work so clear/dealloc paths
//! tolerate partially initialized instances.
//!
//! ## Thread Safety
//!
//! - `ultarNextRow` releases the GIL during blocking I/O.
//! - `ultarReclaimRow` is called with GIL held (from `tp_dealloc`).
//! - Native row buffer pool is protected by `row_buf_mutex` in `LuaDataLoader`.

const std = @import("std");
const lua_dataloader = @import("lua_dataloader");

// Import types and functions from lua_dataloader
const LuaLoaderSpec = lua_dataloader.LuaLoaderSpec;
const LuaLoaderCCtx = lua_dataloader.LuaLoaderCCtx;
const LoadedRow = lua_dataloader.LoadedRow;

// Import Python C API using official headers
// We use Py_LIMITED_API 0x030b0000 (Python 3.11+) which includes Py_buffer in stable ABI
const py = @cImport({
    @cDefine("Py_LIMITED_API", "0x030b0000"); // Python 3.11+
    @cInclude("Python.h");
});

const zeros = std.mem.zeroes;

// ABI3-safe type checking helpers (avoid *_Check macros that inspect object headers)
inline fn isUnicode(obj: ?*py.PyObject) bool {
    if (obj) |o| {
        return py.PyObject_IsInstance(o, @ptrCast(@alignCast(&py.PyUnicode_Type))) == 1;
    }
    return false;
}

inline fn isLong(obj: ?*py.PyObject) bool {
    if (obj) |o| {
        return py.PyObject_IsInstance(o, @ptrCast(@alignCast(&py.PyLong_Type))) == 1;
    }
    return false;
}

inline fn isDict(obj: ?*py.PyObject) bool {
    if (obj) |o| {
        return py.PyObject_IsInstance(o, @ptrCast(@alignCast(&py.PyDict_Type))) == 1;
    }
    return false;
}

const ModuleState = struct {
    data_loader_type: ?*py.PyTypeObject = null,
    loaded_row_type: ?*py.PyTypeObject = null,
};

inline fn moduleState(module: *py.PyObject) *ModuleState {
    return @ptrCast(@alignCast(py.PyModule_GetState(module).?));
}

inline fn moduleStateFromType(typ: *py.PyTypeObject) *ModuleState {
    return @ptrCast(@alignCast(py.PyType_GetModuleState(typ).?));
}

inline fn loadedRowType(parent: *DataLoaderObject) PyError!*py.PyTypeObject {
    const parent_type = parent.typ orelse return error.RuntimeError;
    return moduleStateFromType(parent_type).loaded_row_type orelse return error.RuntimeError;
}

fn freeHeapTypeInstance(typ: ?*py.PyTypeObject, self_obj: ?*py.PyObject) void {
    const heap_type = typ orelse return;
    const free_fn = py.PyType_GetSlot(heap_type, py.Py_tp_free) orelse unreachable;
    const free: *const fn (?*anyopaque) callconv(.c) void = @ptrCast(@alignCast(free_fn));
    free(self_obj);
    py.Py_DecRef(@ptrCast(@alignCast(heap_type)));
}

// Our DataLoader object
const DataLoaderObject = extern struct {
    ob_base: py.PyObject,
    typ: ?*py.PyTypeObject,
    loader: ?*LuaLoaderCCtx,
};

// Our LoadedRow object (represents a single row from the dataloader)
const LoadedRowObject = extern struct {
    ob_base: py.PyObject,
    typ: ?*py.PyTypeObject,
    parent: ?*DataLoaderObject, // Keep reference to parent
    row: ?*LoadedRow,
};

// Slot definitions for DataLoader type
const DataLoader_slots = [_]py.PyType_Slot{
    .{ .slot = py.Py_tp_new, .pfunc = @ptrCast(@constCast(&dataLoaderNew)) },
    .{ .slot = py.Py_tp_dealloc, .pfunc = @ptrCast(@constCast(&dataLoaderDealloc)) },
    .{ .slot = py.Py_tp_repr, .pfunc = @ptrCast(@constCast(&dataLoaderRepr)) },
    .{ .slot = py.Py_tp_iter, .pfunc = @ptrCast(@constCast(&dataLoaderIter)) },
    .{ .slot = py.Py_tp_iternext, .pfunc = @ptrCast(@constCast(&dataLoaderNext)) },
    .{ .slot = py.Py_tp_doc, .pfunc = @ptrCast(@constCast("Ultar DataLoader - async Lua-scripted data loading")) },
    zeros(py.PyType_Slot), // Sentinel
};

var DataLoader_spec = py.PyType_Spec{
    .name = "ultar_dataloader._native.DataLoader",
    .basicsize = @sizeOf(DataLoaderObject),
    .itemsize = 0,
    .flags = py.Py_TPFLAGS_DEFAULT,
    .slots = @ptrCast(@constCast(&DataLoader_slots)),
};

// Slot definitions for LoadedRow type
const LoadedRow_slots = [_]py.PyType_Slot{
    .{ .slot = py.Py_tp_dealloc, .pfunc = @ptrCast(@constCast(&loadedRowDealloc)) },
    .{ .slot = py.Py_tp_repr, .pfunc = @ptrCast(@constCast(&loadedRowRepr)) },
    .{ .slot = py.Py_tp_methods, .pfunc = @ptrCast(@constCast(&LoadedRow_methods)) },
    .{ .slot = py.Py_sq_length, .pfunc = @ptrCast(@constCast(&loadedRowLen)) },
    .{ .slot = py.Py_mp_length, .pfunc = @ptrCast(@constCast(&loadedRowLen)) },
    .{ .slot = py.Py_mp_subscript, .pfunc = @ptrCast(@constCast(&loadedRowSubscript)) },
    .{ .slot = py.Py_tp_doc, .pfunc = @ptrCast(@constCast("LoadedRow - a row of data from the DataLoader")) },
    zeros(py.PyType_Slot), // Sentinel
};

var LoadedRow_spec = py.PyType_Spec{
    .name = "ultar_dataloader._native.LoadedRow",
    .basicsize = @sizeOf(LoadedRowObject),
    .itemsize = 0,
    .flags = py.Py_TPFLAGS_DEFAULT,
    .slots = @ptrCast(@constCast(&LoadedRow_slots)),
};

// Method definitions
const LoadedRow_methods = [_]py.PyMethodDef{
    .{
        .ml_name = "keys",
        .ml_meth = @ptrCast(&loadedRowKeys),
        .ml_flags = py.METH_NOARGS,
        .ml_doc = "Return list of keys in this row",
    },
    .{
        .ml_name = "items",
        .ml_meth = @ptrCast(&loadedRowItems),
        .ml_flags = py.METH_NOARGS,
        .ml_doc = "Return list of (key, bytes) tuples",
    },
    .{
        .ml_name = "to_dict",
        .ml_meth = @ptrCast(&loadedRowToDict),
        .ml_flags = py.METH_NOARGS,
        .ml_doc = "Return dict mapping keys to bytes",
    },
    zeros(py.PyMethodDef),
};

// ============================================================================
// Error types for Zig-native functions
// ============================================================================

const PyError = error{
    /// Python exception already set - just propagate
    PythonException,
    /// Type error
    TypeError,
    /// Memory allocation failed
    OutOfMemory,
    /// Runtime error
    RuntimeError,
};

/// Set Python exception based on Zig error
fn setPyError(err: PyError, msg: [*:0]const u8) void {
    switch (err) {
        error.PythonException => {}, // Already set
        error.TypeError => py.PyErr_SetString(py.PyExc_TypeError, msg),
        error.OutOfMemory => py.PyErr_SetString(py.PyExc_MemoryError, msg),
        error.RuntimeError => py.PyErr_SetString(py.PyExc_RuntimeError, msg),
    }
}

// ============================================================================
// Zig-native implementation functions (proper error handling)
// ============================================================================

const ConfigArrays = struct {
    keys: [][*:0]const u8,
    values: [][*:0]const u8,
};

/// Parse a Python Mapping into arena-allocated key/value arrays
fn parseConfigDictImpl(arena: std.mem.Allocator, config_obj: *py.PyObject) PyError!?ConfigArrays {
    // Check it's a dict-like object
    if (!isDict(config_obj)) {
        const keys_method = py.PyObject_GetAttrString(config_obj, "keys");
        if (keys_method == null) return error.TypeError;
        py.Py_DecRef(keys_method);
    }

    const items = py.PyMapping_Items(config_obj) orelse return error.TypeError;
    defer py.Py_DecRef(items);

    const count: usize = @intCast(py.PyList_Size(items));
    if (count == 0) return null;

    const keys = try arena.alloc([*:0]const u8, count);
    const values = try arena.alloc([*:0]const u8, count);

    for (0..count) |i| {
        const item = py.PyList_GetItem(items, @intCast(i));
        const key_obj = py.PyTuple_GetItem(item, 0);
        const val_obj = py.PyTuple_GetItem(item, 1);

        // Get key as string
        var key_len: py.Py_ssize_t = 0;
        const key_ptr = py.PyUnicode_AsUTF8AndSize(key_obj, &key_len) orelse return error.TypeError;
        keys[i] = (try arena.dupeZ(u8, key_ptr[0..@intCast(key_len)])).ptr;

        // Get value as string (convert if needed)
        var val_len: py.Py_ssize_t = 0;
        const val_slice = blk: {
            if (isUnicode(val_obj)) {
                const ptr = py.PyUnicode_AsUTF8AndSize(val_obj, &val_len) orelse return error.TypeError;
                break :blk ptr[0..@intCast(val_len)];
            } else {
                const val_str = py.PyObject_Str(val_obj) orelse return error.PythonException;
                defer py.Py_DecRef(val_str);
                const ptr = py.PyUnicode_AsUTF8AndSize(val_str, &val_len) orelse return error.TypeError;
                break :blk ptr[0..@intCast(val_len)];
            }
        };
        values[i] = (try arena.dupeZ(u8, val_slice)).ptr;
    }

    return .{ .keys = keys, .values = values };
}

/// Create a DataLoader - Zig-native implementation
fn dataLoaderNewImpl(
    arena: std.mem.Allocator,
    typ: *py.PyTypeObject,
    src_obj: *py.PyObject,
    config_obj: ?*py.PyObject,
    rank: c_uint,
    world_size: c_uint,
    debug: bool,
) PyError!*DataLoaderObject {
    // Get and copy src string
    var src_len: py.Py_ssize_t = 0;
    const src_ptr = py.PyUnicode_AsUTF8AndSize(src_obj, &src_len) orelse return error.TypeError;
    const src_copy = try arena.dupeZ(u8, src_ptr[0..@intCast(src_len)]);

    // Parse config dict if provided
    const config = if (config_obj != null and config_obj != py.Py_None())
        try parseConfigDictImpl(arena, config_obj.?)
    else
        null;

    // Build the spec
    const spec = lua_dataloader.LuaLoaderSpec{
        .src = src_copy.ptr,
        .shard_list = null,
        .num_shards = 0,
        .rank = rank,
        .world_size = world_size,
        .debug = debug,
        .config_keys = if (config) |c| @ptrCast(c.keys.ptr) else null,
        .config_values = if (config) |c| @ptrCast(c.values.ptr) else null,
        .config_count = if (config) |c| @intCast(c.keys.len) else 0,
    };

    // Allocate Python object
    const alloc_fn = py.PyType_GetSlot(typ, py.Py_tp_alloc) orelse return error.RuntimeError;
    const alloc: *const fn (?*py.PyTypeObject, py.Py_ssize_t) callconv(.c) ?*py.PyObject = @ptrCast(@alignCast(alloc_fn));
    const self_obj = alloc(typ, 0) orelse return error.PythonException;

    const self: *DataLoaderObject = @ptrCast(@alignCast(self_obj));
    self.typ = typ;
    self.loader = null;
    errdefer freeHeapTypeInstance(self.typ, self_obj);

    // Release GIL during heavy initialization
    const gil_state = py.PyEval_SaveThread();
    const loader = lua_dataloader.ultarCreateLuaLoader(spec);
    py.PyEval_RestoreThread(gil_state);

    if (loader == null) return error.RuntimeError;

    self.loader = loader;
    return self;
}

// ============================================================================
// C ABI wrappers (handle errors, set Python exceptions)
// ============================================================================

fn dataLoaderNew(typ: ?*py.PyTypeObject, args: ?*py.PyObject, kwargs: ?*py.PyObject) callconv(.c) ?*py.PyObject {
    var src_obj: ?*py.PyObject = null;
    var config_obj: ?*py.PyObject = null;
    var rank: c_uint = 0;
    var world_size: c_uint = 1;
    var debug: c_int = 0;

    const kwlist = [_:null]?[*:0]const u8{ "src", "config", "rank", "world_size", "debug", null };

    if (py.PyArg_ParseTupleAndKeywords(
        args,
        kwargs,
        "O|OIIp",
        @ptrCast(@constCast(&kwlist)),
        &src_obj,
        &config_obj,
        &rank,
        &world_size,
        &debug,
    ) == 0) {
        return null;
    }

    // Arena for all temporary allocations - freed after createLoader copies everything
    var arena_state = std.heap.ArenaAllocator.init(std.heap.c_allocator);
    defer arena_state.deinit();

    const self = dataLoaderNewImpl(
        arena_state.allocator(),
        typ.?,
        src_obj.?,
        config_obj,
        rank,
        world_size,
        debug != 0,
    ) catch |err| {
        switch (err) {
            error.PythonException => {},
            error.TypeError => py.PyErr_SetString(py.PyExc_TypeError, "Invalid argument type"),
            error.OutOfMemory => py.PyErr_SetString(py.PyExc_MemoryError, "Out of memory"),
            error.RuntimeError => py.PyErr_SetString(py.PyExc_RuntimeError, "Failed to create DataLoader - check Lua script"),
        }
        return null;
    };

    return @ptrCast(self);
}

fn dataLoaderClear(self: *DataLoaderObject) void {
    if (self.loader) |loader| {
        self.loader = null;
        lua_dataloader.ultarDestroyLuaLoader(loader);
    }
}

fn dataLoaderDealloc(self_obj: ?*py.PyObject) callconv(.c) void {
    const self: *DataLoaderObject = @ptrCast(@alignCast(self_obj));

    dataLoaderClear(self);
    const typ = self.typ;
    self.typ = null;
    freeHeapTypeInstance(typ, self_obj);
}

fn dataLoaderRepr(_: ?*py.PyObject) callconv(.c) ?*py.PyObject {
    return py.PyUnicode_FromString("<DataLoader>");
}

fn dataLoaderIter(self_obj: ?*py.PyObject) callconv(.c) ?*py.PyObject {
    py.Py_IncRef(self_obj);
    return self_obj;
}

fn dataLoaderNext(self_obj: ?*py.PyObject) callconv(.c) ?*py.PyObject {
    const self: *DataLoaderObject = @ptrCast(@alignCast(self_obj));

    const loader = self.loader orelse {
        py.PyErr_SetString(py.PyExc_RuntimeError, "DataLoader not initialized");
        return null;
    };

    // Release GIL during blocking I/O
    const gil_state = py.PyEval_SaveThread();
    const row = lua_dataloader.ultarNextRow(loader);
    py.PyEval_RestoreThread(gil_state);

    // Create a LoadedRow object, or signal StopIteration if done
    const valid_row = row orelse {
        py.PyErr_SetNone(py.PyExc_StopIteration);
        return null;
    };

    return wrapOwnedRow(self, valid_row) catch |err| {
        lua_dataloader.ultarReclaimRow(loader, valid_row);
        switch (err) {
            error.PythonException => {},
            error.RuntimeError => py.PyErr_SetString(py.PyExc_RuntimeError, "Failed to create LoadedRow"),
            error.TypeError => py.PyErr_SetString(py.PyExc_TypeError, "Type error creating LoadedRow"),
            error.OutOfMemory => py.PyErr_SetString(py.PyExc_MemoryError, "Out of memory"),
        }
        return null;
    };
}

/// Wrap a native LoadedRow in a Python object. **Takes ownership of `row`.**
///
/// On success: The returned Python object owns `row` and will reclaim it on dealloc.
/// On failure: `row` is returned, caller must handle reclaim.
fn wrapOwnedRow(parent: *DataLoaderObject, row: *LoadedRow) PyError!*py.PyObject {
    const typ = try loadedRowType(parent);

    const alloc_fn = py.PyType_GetSlot(typ, py.Py_tp_alloc) orelse return error.RuntimeError;
    const alloc: *const fn (?*py.PyTypeObject, py.Py_ssize_t) callconv(.c) ?*py.PyObject = @ptrCast(@alignCast(alloc_fn));
    const self_obj = alloc(typ, 0) orelse return error.PythonException;

    const row_obj: *LoadedRowObject = @ptrCast(@alignCast(self_obj));
    row_obj.typ = typ;
    row_obj.parent = null;
    row_obj.row = null;
    row_obj.parent = parent;
    row_obj.row = row;

    // Keep parent alive
    py.Py_IncRef(@ptrCast(parent));
    return self_obj;
}

fn loadedRowClear(self: *LoadedRowObject) void {
    const row_to_reclaim = self.row;
    self.row = null;

    if (self.parent) |parent| {
        self.parent = null;
        if (row_to_reclaim) |row| {
            if (parent.loader) |loader| {
                lua_dataloader.ultarReclaimRow(loader, row);
            }
        }
        py.Py_DecRef(@ptrCast(parent));
    }
}

fn loadedRowDealloc(self_obj: ?*py.PyObject) callconv(.c) void {
    const self: *LoadedRowObject = @ptrCast(@alignCast(self_obj));

    loadedRowClear(self);
    const typ = self.typ;
    self.typ = null;
    freeHeapTypeInstance(typ, self_obj);
}

fn loadedRowRepr(self_obj: ?*py.PyObject) callconv(.c) ?*py.PyObject {
    const self: *LoadedRowObject = @ptrCast(@alignCast(self_obj));
    const row = self.row orelse {
        return py.PyUnicode_FromString("<LoadedRow (invalid)>");
    };

    var buf: [128]u8 = undefined;
    const len = std.fmt.bufPrint(&buf, "<LoadedRow with {d} entries>", .{row.num_keys}) catch {
        return py.PyUnicode_FromString("<LoadedRow>");
    };

    return py.PyUnicode_FromStringAndSize(len.ptr, @intCast(len.len));
}

fn loadedRowLen(self_obj: ?*py.PyObject) callconv(.c) py.Py_ssize_t {
    const self: *LoadedRowObject = @ptrCast(@alignCast(self_obj));
    if (self.row) |row| {
        return @intCast(row.num_keys);
    }
    return 0;
}

/// Get entry data as a Python bytes object (copies data).
/// Memory-safe: the bytes object owns its data independently of the LoadedRow.
fn getEntryBytes(row: *LoadedRow, idx: usize) ?*py.PyObject {
    const data_ptr: [*]const u8 = @ptrCast(row.data[idx]);
    const size: py.Py_ssize_t = @intCast(row.sizes[idx]);
    return py.PyBytes_FromStringAndSize(@ptrCast(data_ptr), size);
}

fn loadedRowSubscript(self_obj: ?*py.PyObject, key: ?*py.PyObject) callconv(.c) ?*py.PyObject {
    const self: *LoadedRowObject = @ptrCast(@alignCast(self_obj));

    const row = self.row orelse {
        py.PyErr_SetString(py.PyExc_RuntimeError, "LoadedRow not initialized");
        return null;
    };

    // Check if key is an integer (index access)
    if (isLong(key)) {
        var idx = py.PyLong_AsSsize_t(key);
        if (idx < 0) {
            idx += @intCast(row.num_keys);
        }
        if (idx < 0 or idx >= @as(py.Py_ssize_t, @intCast(row.num_keys))) {
            py.PyErr_SetString(py.PyExc_IndexError, "index out of range");
            return null;
        }
        return getEntryBytes(row, @intCast(idx));
    }

    // String key access
    var key_len: py.Py_ssize_t = 0;
    const key_str = py.PyUnicode_AsUTF8AndSize(key, &key_len) orelse {
        py.PyErr_SetString(py.PyExc_TypeError, "Key must be a string or integer");
        return null;
    };

    const key_slice: []const u8 = key_str[0..@intCast(key_len)];

    // Linear search for key (rows are typically small)
    for (0..row.num_keys) |i| {
        const entry_key: [*:0]const u8 = @ptrCast(row.keys[i]);
        const entry_key_slice = std.mem.span(entry_key);
        if (std.mem.eql(u8, entry_key_slice, key_slice)) {
            return getEntryBytes(row, i);
        }
    }

    _ = py.PyErr_Format(py.PyExc_KeyError, "%s", key_str);
    return null;
}

fn loadedRowKeys(self_obj: ?*py.PyObject, _: ?*py.PyObject) callconv(.c) ?*py.PyObject {
    const self: *LoadedRowObject = @ptrCast(@alignCast(self_obj.?));

    const row = self.row orelse {
        py.PyErr_SetString(py.PyExc_RuntimeError, "LoadedRow not initialized");
        return null;
    };

    const list = py.PyList_New(@intCast(row.num_keys)) orelse return null;

    for (0..row.num_keys) |i| {
        const key: [*:0]const u8 = @ptrCast(row.keys[i]);
        const key_slice = std.mem.span(key);
        const name = py.PyUnicode_FromStringAndSize(key_slice.ptr, @intCast(key_slice.len)) orelse {
            py.Py_DecRef(list);
            return null;
        };
        _ = py.PyList_SetItem(list, @intCast(i), name);
    }

    return list;
}

fn loadedRowItems(self_obj: ?*py.PyObject, _: ?*py.PyObject) callconv(.c) ?*py.PyObject {
    const self: *LoadedRowObject = @ptrCast(@alignCast(self_obj.?));

    const row = self.row orelse {
        py.PyErr_SetString(py.PyExc_RuntimeError, "LoadedRow not initialized");
        return null;
    };

    const list = py.PyList_New(@intCast(row.num_keys)) orelse return null;

    for (0..row.num_keys) |i| {
        const key: [*:0]const u8 = @ptrCast(row.keys[i]);
        const key_slice = std.mem.span(key);
        const name = py.PyUnicode_FromStringAndSize(key_slice.ptr, @intCast(key_slice.len)) orelse {
            py.Py_DecRef(list);
            return null;
        };

        const data_ptr: [*]const u8 = @ptrCast(row.data[i]);
        const size: usize = @intCast(row.sizes[i]);
        const data = py.PyBytes_FromStringAndSize(@ptrCast(data_ptr), @intCast(size)) orelse {
            py.Py_DecRef(name);
            py.Py_DecRef(list);
            return null;
        };

        const tuple = py.PyTuple_Pack(2, name, data) orelse {
            py.Py_DecRef(name);
            py.Py_DecRef(data);
            py.Py_DecRef(list);
            return null;
        };

        py.Py_DecRef(name);
        py.Py_DecRef(data);
        _ = py.PyList_SetItem(list, @intCast(i), tuple);
    }

    return list;
}

fn loadedRowToDict(self_obj: ?*py.PyObject, _: ?*py.PyObject) callconv(.c) ?*py.PyObject {
    const self: *LoadedRowObject = @ptrCast(@alignCast(self_obj.?));

    const row = self.row orelse {
        py.PyErr_SetString(py.PyExc_RuntimeError, "LoadedRow not initialized");
        return null;
    };

    const dict = py.PyDict_New() orelse return null;

    for (0..row.num_keys) |i| {
        const key: [*:0]const u8 = @ptrCast(row.keys[i]);
        const key_slice = std.mem.span(key);
        const name = py.PyUnicode_FromStringAndSize(key_slice.ptr, @intCast(key_slice.len)) orelse {
            py.Py_DecRef(dict);
            return null;
        };

        const data_ptr: [*]const u8 = @ptrCast(row.data[i]);
        const size: usize = @intCast(row.sizes[i]);
        const data = py.PyBytes_FromStringAndSize(@ptrCast(data_ptr), @intCast(size)) orelse {
            py.Py_DecRef(name);
            py.Py_DecRef(dict);
            return null;
        };

        if (py.PyDict_SetItem(dict, name, data) < 0) {
            py.Py_DecRef(name);
            py.Py_DecRef(data);
            py.Py_DecRef(dict);
            return null;
        }

        py.Py_DecRef(name);
        py.Py_DecRef(data);
    }

    return dict;
}

// Module definition
const module_methods = [_]py.PyMethodDef{
    std.mem.zeroes(py.PyMethodDef),
};

fn moduleTraverse(module_obj: ?*py.PyObject, visit: py.visitproc, arg: ?*anyopaque) callconv(.c) c_int {
    const state = moduleState(module_obj.?);

    if (state.data_loader_type) |typ| {
        if (visit.?(@ptrCast(@alignCast(typ)), arg) != 0) return -1;
    }
    if (state.loaded_row_type) |typ| {
        if (visit.?(@ptrCast(@alignCast(typ)), arg) != 0) return -1;
    }
    return 0;
}

fn moduleClear(module_obj: ?*py.PyObject) callconv(.c) c_int {
    const state = moduleState(module_obj.?);

    if (state.data_loader_type) |typ| {
        state.data_loader_type = null;
        py.Py_DecRef(@ptrCast(@alignCast(typ)));
    }
    if (state.loaded_row_type) |typ| {
        state.loaded_row_type = null;
        py.Py_DecRef(@ptrCast(@alignCast(typ)));
    }
    return 0;
}

fn moduleExec(module_obj: ?*py.PyObject) callconv(.c) c_int {
    const module = module_obj orelse return -1;
    const state = moduleState(module);
    state.* = zeros(ModuleState);

    state.data_loader_type = @ptrCast(py.PyType_FromModuleAndSpec(module, &DataLoader_spec, null));
    if (state.data_loader_type == null) return -1;

    state.loaded_row_type = @ptrCast(py.PyType_FromModuleAndSpec(module, &LoadedRow_spec, null));
    if (state.loaded_row_type == null) {
        _ = moduleClear(module_obj);
        return -1;
    }

    if (py.PyModule_AddObjectRef(module, "DataLoader", @ptrCast(@alignCast(state.data_loader_type))) < 0) {
        _ = moduleClear(module_obj);
        return -1;
    }
    if (py.PyModule_AddObjectRef(module, "LoadedRow", @ptrCast(@alignCast(state.loaded_row_type))) < 0) {
        _ = moduleClear(module_obj);
        return -1;
    }

    return 0;
}

const module_slots = [_]py.PyModuleDef_Slot{
    .{ .slot = py.Py_mod_exec, .value = @ptrCast(@constCast(&moduleExec)) },
    zeros(py.PyModuleDef_Slot),
};

var module_def: py.PyModuleDef = undefined;
var module_def_initialized = false;

fn getModuleDef() *py.PyModuleDef {
    if (!module_def_initialized) {
        const size = @sizeOf(py.PyModuleDef);
        const ptr: [*]u8 = @ptrCast(&module_def);
        @memset(ptr[0..size], 0);

        module_def.m_name = "ultar_dataloader._native";
        module_def.m_doc = "Fast async dataloader with Lua scripting (Zig implementation)";
        module_def.m_size = @sizeOf(ModuleState);
        module_def.m_methods = @ptrCast(@constCast(&module_methods));
        module_def.m_slots = @ptrCast(@constCast(&module_slots));
        module_def.m_traverse = &moduleTraverse;
        module_def.m_clear = &moduleClear;
        module_def_initialized = true;
    }
    return &module_def;
}

export fn PyInit__native() ?*py.PyObject {
    return py.PyModuleDef_Init(getModuleDef());
}
