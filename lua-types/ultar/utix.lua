---@meta

---@class ultar.UtixRow
---A row from a .utix index file
---@field keys string[] Entry keys in this row (e.g., {".json", ".png", ".safetensors"})
---@field offsets integer[] Relative byte offsets for each entry within the tar record
---@field sizes integer[] Sizes in bytes for each entry
---@field offset integer Base byte offset of this row in the tar file
local UtixRow = {}

---@class ultar.UtixReader
---Iterator-capable reader for .utix (msgpack) index files
local UtixReader = {}

---Iterate over rows in the index file.
---@return fun(): ultar.UtixRow? iterator Iterator function returning rows
function UtixReader:iter() end

---@class ultar.utix
---UTIX/msgpack index file reader module
local utix = {}

---Open a .utix index file and return a reader.
---@param path string Absolute path to the .utix file
---@return ultar.UtixReader reader Reader object with iter() method
function utix.open(path) end

return utix

