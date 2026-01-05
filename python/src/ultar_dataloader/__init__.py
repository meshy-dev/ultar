"""
ultar_dataloader - Fast async dataloader with Lua scripting

This library provides a high-performance dataloader implemented in Zig,
with Lua scripting for flexible data loading pipelines.

The native extension properly manages memory and integrates with Python's
garbage collector, avoiding the GC-related issues with ctypes bindings.
"""

from __future__ import annotations

from ultar_dataloader._version import __version__

from collections.abc import Mapping
from pathlib import Path
from typing import TYPE_CHECKING, Iterator

# Import from the native extension module
from ultar_dataloader._native import DataLoader as _DataLoader
from ultar_dataloader._native import LoadedRow as _LoadedRow

if TYPE_CHECKING:
    import torch


class LoadedRow:
    """
    A row of data loaded by the DataLoader.

    Provides dict-like access to the data entries by key name or index.
    Each entry is returned as a ``bytes`` object.
    """

    __slots__ = ("_row",)

    def __init__(self, row: _LoadedRow):
        self._row = row

    def keys(self) -> list[str]:
        """Return list of keys in this row."""
        return self._row.keys()

    def items(self) -> list[tuple[str, bytes]]:
        """Return list of (key, bytes) tuples."""
        return self._row.items()

    def to_dict(self) -> dict[str, bytes]:
        """Return dict mapping keys to bytes."""
        return self._row.to_dict()

    def __len__(self) -> int:
        return len(self._row)

    def __contains__(self, key: str) -> bool:
        try:
            _ = self._row[key]
            return True
        except KeyError:
            return False

    def __getitem__(self, key: str | int) -> bytes:
        """Get entry data as bytes."""
        return self._row[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self.keys())

    def __repr__(self) -> str:
        return f"<LoadedRow with {len(self)} entries: {self.keys()}>"


class DataLoader:
    """
    High-performance async dataloader with Lua scripting.

    This dataloader uses io_uring (via libxev) for async I/O and Lua
    coroutines for flexible data loading logic. It avoids the GC-related
    issues of ctypes bindings by using Python's native extension API.

    The optional `config` parameter allows passing a Python dict to Lua.
    It is passed as the 3rd argument to `init_ctx(rank, world_size, config)`.

    Example:
        >>> lua_script = '''
        ... return {
        ...     init_ctx = function(rank, world_size, config)
        ...         -- Config passed from Python as 3rd argument
        ...         return {
        ...             tar_path = config.tar_path,
        ...             idx_path = config.idx_path,
        ...         }
        ...     end,
        ...     row_generator = function(ctx)
        ...         local f = g_loader:open_file(ctx.tar_path)
        ...         -- ... load data ...
        ...         g_loader:close_file(f)
        ...     end,
        ... }
        ... '''
        >>> loader = DataLoader(
        ...     lua_script,
        ...     config={"tar_path": "/path/to/data.tar", "idx_path": "/path/to/data.tar.utix"},
        ...     rank=0,
        ...     world_size=1,
        ... )
        >>> for row in loader:
        ...     print(row.keys())
    """

    __slots__ = ("_loader",)

    def __init__(
        self,
        src: str,
        config: Mapping[str, str] | None = None,
        rank: int = 0,
        world_size: int = 1,
        debug: bool = False,
    ):
        """
        Create a new DataLoader.

        Args:
            src: Lua script source code defining the loading logic.
                 Must return a table with `init_ctx` and `row_generator` functions.
            config: Optional dict-like mapping of string keys to values.
                    Available in Lua as the global `g_config` table.
                    Values are converted to strings.
            rank: Current process rank (for distributed training).
            world_size: Total number of processes (for distributed training).
            debug: Enable debug mode with additional logging and checks.
        """
        self._loader = _DataLoader(
            src=src,
            config=dict(config) if config is not None else None,
            rank=rank,
            world_size=world_size,
            debug=debug,
        )

    @classmethod
    def from_file(
        cls,
        script_path: str | Path,
        config: Mapping[str, str] | None = None,
        rank: int = 0,
        world_size: int = 1,
        debug: bool = False,
    ) -> "DataLoader":
        """
        Create a DataLoader from a Lua script file.

        Args:
            script_path: Path to the Lua script file.
            config: Optional dict-like mapping of string keys to values.
                    Available in Lua as the global `g_config` table.
            rank: Current process rank (for distributed training).
            world_size: Total number of processes (for distributed training).
            debug: Enable debug mode with additional logging and checks.

        Returns:
            DataLoader instance.
        """
        with open(script_path, "r") as f:
            src = f.read()
        return cls(src=src, config=config, rank=rank, world_size=world_size, debug=debug)

    def __iter__(self) -> Iterator[LoadedRow]:
        for row in self._loader:
            yield LoadedRow(row)

    def __repr__(self) -> str:
        return "<DataLoader>"


__all__ = [
    "DataLoader",
    "LoadedRow",
]
