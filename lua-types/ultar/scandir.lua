---@meta

---@class ultar.ScanEntry
---An entry from directory scanning
---@field name string File or directory name
---@field kind "file"|"directory"|"symlink"|"unknown" Entry type
local ScanEntry = {}

---@class ultar.ScanCtx
---Directory scanner context
local ScanCtx = {}

---Iterate over entries in the directory.
---@return fun(): string? iterator Iterator function returning full paths
function ScanCtx:iter() end

---@class ultar.scandir
---Directory scanner module
local scandir = {}

---Scan a directory and return an iterator context.
---@param path string Absolute path to the directory
---@return ultar.ScanCtx ctx Scanner context with iter() method
function scandir.open(path) end

return scandir

