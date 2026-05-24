---@meta

---@class ultar.FileHandle
---Opaque file handle returned by loader:open_file()
local FileHandle = {}

---@class ultar.loader
---The loader interface for async data loading.
---Methods that perform I/O yield; data emission (add_entry_bytes) is synchronous.
---Use colon syntax for method calls: `loader:open_file(path)`
local loader = {}

---Open a file for reading.
---This is a yielding operation - it will suspend the coroutine until the file is opened.
---@param path string Absolute path to the file
---@return ultar.FileHandle handle Opaque file handle for use with other loader methods
function loader:open_file(path) end

---Close a previously opened file.
---@param handle ultar.FileHandle The file handle returned by open_file
---@return nil
function loader:close_file(handle) end

---Add an entry to the current row being built.
---Call this multiple times to add entries, then call finish_row().
---@param handle ultar.FileHandle File handle containing the data
---@param key string Entry key name (e.g., ".json", ".png")
---@param offset integer Byte offset in the file where the entry data starts
---@param size integer Size of the entry data in bytes
---@return nil
function loader:add_entry(handle, key, offset, size) end

---Add in-memory bytes entry (synchronous, no yield/IO). Mix with add_entry before finish_row. Binary-safe.
---@param key string Entry key name (e.g., ".synthetic")
---@param data string Raw bytes content (binary safe, may contain \0)
---@return nil
function loader:add_entry_bytes(key, data) end

---Finish the current row and make it available to Python.
---After calling this, start building a new row with add_entry() calls.
---@return nil
function loader:finish_row() end

return loader
