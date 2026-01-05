"""Type stubs for the native ultar_dataloader extension module."""

from typing import Iterator

class LoadedRow:
    """A row of data from the DataLoader - supports dict-like access."""

    def keys(self) -> list[str]:
        """Return list of keys in this row."""
        ...

    def items(self) -> list[tuple[str, bytes]]:
        """Return list of (key, bytes) tuples."""
        ...

    def to_dict(self) -> dict[str, bytes]:
        """Return dict mapping keys to bytes."""
        ...

    def __len__(self) -> int:
        """Return number of entries in this row."""
        ...

    def __getitem__(self, key: str | int) -> bytes:
        """Get entry by key name or index."""
        ...

    def __repr__(self) -> str:
        """Return string representation."""
        ...

class DataLoader:
    """Ultar DataLoader - async Lua-scripted data loading."""

    def __init__(
        self,
        src: str,
        config: dict[str, str] | None = None,
        rank: int = 0,
        world_size: int = 1,
        debug: bool = False,
    ) -> None:
        """
        Create a new DataLoader.

        Args:
            src: Lua script source code defining the loading logic.
            config: Optional dict of string key-value pairs, available as g_config in Lua.
            rank: Current process rank (for distributed training).
            world_size: Total number of processes (for distributed training).
            debug: Enable debug mode.
        """
        ...

    def __iter__(self) -> Iterator[LoadedRow]:
        """Iterate over rows from the dataloader."""
        ...

    def __next__(self) -> LoadedRow:
        """Get the next row from the dataloader."""
        ...

    def __repr__(self) -> str:
        """Return string representation."""
        ...
