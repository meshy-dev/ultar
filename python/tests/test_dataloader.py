"""
Test the ultar_dataloader package with real tar files.
"""

import time
from pathlib import Path

from ultar_dataloader import DataLoader

# Test with the avocado dataset
DATA_DIR = Path("/data/datasets/meshylatv/avocado_20251017/val")
INDEX_FILE = DATA_DIR / "shard_0000_of_0256.base.tar.utix"
TAR_FILE = DATA_DIR / "shard_0000_of_0256.base.tar"

# Load Lua script from external file
SCRIPT_FILE = Path(__file__).parent / "loader_script.lua"
LUA_SCRIPT = SCRIPT_FILE.read_text()


def test_basic_loading():
    """Test basic data loading from tar file."""
    if not INDEX_FILE.exists():
        print(f"Skipping test: {INDEX_FILE} not found")
        return

    print("Creating DataLoader...")
    loader = DataLoader(
        src=LUA_SCRIPT,
        config={
            "tar_path": str(TAR_FILE),
            "idx_path": str(INDEX_FILE),
            "max_rows": "10",
        },
        rank=0,
        world_size=1,
    )

    print("Loading rows...")
    rows_loaded = 0
    total_bytes = 0

    for row in loader:
        rows_loaded += 1
        keys = row.keys()
        print(f"  Row {rows_loaded}: {len(keys)} entries - {keys[:3]}...")

        # Access each entry
        for key in keys:
            data = row[key]
            total_bytes += len(data)

    print(f"Loaded {rows_loaded} rows, {total_bytes / 1024:.1f} KB total")
    assert rows_loaded == 10, f"Expected 10 rows, got {rows_loaded}"


def test_performance():
    """Test loading performance with more rows."""
    if not INDEX_FILE.exists():
        print(f"Skipping test: {INDEX_FILE} not found")
        return

    loader = DataLoader(
        src=LUA_SCRIPT,
        config={
            "tar_path": str(TAR_FILE),
            "idx_path": str(INDEX_FILE),
            "max_rows": "100",
        },
    )

    start = time.perf_counter()
    rows = 0
    total_bytes = 0

    for row in loader:
        rows += 1
        for key in row.keys():
            total_bytes += len(row[key])

    elapsed = time.perf_counter() - start
    mb = total_bytes / (1024 * 1024)

    print(f"Performance: {rows} rows, {mb:.1f} MB in {elapsed:.2f}s")
    print(f"  Throughput: {mb / elapsed:.1f} MB/s, {rows / elapsed:.1f} rows/s")


def test_dict_access():
    """Test dict-like access to row data."""
    if not INDEX_FILE.exists():
        print(f"Skipping test: {INDEX_FILE} not found")
        return

    loader = DataLoader(
        src=LUA_SCRIPT,
        config={
            "tar_path": str(TAR_FILE),
            "idx_path": str(INDEX_FILE),
            "max_rows": "1",
        },
    )

    for row in loader:
        # Test keys()
        keys = row.keys()
        print(f"Keys: {keys}")

        # Test __len__
        print(f"Length: {len(row)}")
        assert len(row) == len(keys)

        # Test __getitem__ by key
        for k in keys:
            data = row[k]
            assert isinstance(data, bytes)
            print(f"  {k}: {len(data)} bytes")

        # Test __getitem__ by index
        data0 = row[0]
        assert data0 == row[keys[0]]

        # Test negative index
        data_last = row[-1]
        assert data_last == row[keys[-1]]

        # Test __contains__
        assert keys[0] in row
        assert "nonexistent_key" not in row

        # Test to_dict()
        d = row.to_dict()
        assert isinstance(d, dict)
        assert set(d.keys()) == set(keys)

        # Test items()
        items = row.items()
        assert len(items) == len(keys)

        print("All dict-like access tests passed!")
        break


def test_config_with_mapping():
    """Test that config accepts any Mapping-like object."""
    if not INDEX_FILE.exists():
        print(f"Skipping test: {INDEX_FILE} not found")
        return

    from collections import OrderedDict

    # Test with OrderedDict
    config = OrderedDict([
        ("tar_path", str(TAR_FILE)),
        ("idx_path", str(INDEX_FILE)),
        ("max_rows", "2"),
    ])

    loader = DataLoader(src=LUA_SCRIPT, config=config)

    rows = list(loader)
    assert len(rows) == 2, f"Expected 2 rows, got {len(rows)}"
    print(f"Config with OrderedDict: loaded {len(rows)} rows")


if __name__ == "__main__":
    print("=" * 60)
    print("Test: Basic Loading")
    print("=" * 60)
    test_basic_loading()

    print()
    print("=" * 60)
    print("Test: Dict Access")
    print("=" * 60)
    test_dict_access()

    print()
    print("=" * 60)
    print("Test: Config with Mapping")
    print("=" * 60)
    test_config_with_mapping()

    print()
    print("=" * 60)
    print("Test: Performance")
    print("=" * 60)
    test_performance()
