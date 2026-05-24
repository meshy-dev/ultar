#!/usr/bin/env python3
"""Demonstrate DataLoader filtering using metadata from an ImageNet-12k .utix file.

Run the benchmark first, or otherwise create the index with:
    ./zig-out/bin/indexer --meta-rule '.json:.label;.width;.height;.filename' /tmp/imagenet12k-wds/imagenet12k-train-0000.tar

Example:
    uv run python load_imagenet_metadata_filtered.py /tmp/imagenet12k-wds/imagenet12k-train-0000.tar
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("tar_path", type=Path, help="WebDataset tar shard")
    parser.add_argument(
        "--idx-path", type=Path, help="matching .utix path; defaults to <tar_path>.utix"
    )
    parser.add_argument(
        "--lua", type=Path, default=Path("metadata_resolution_filter.lua")
    )
    parser.add_argument("--max-rows", type=int, default=5)
    parser.add_argument(
        "--max-dimension",
        type=int,
        default=1024,
        help="skip rows wider or taller than this",
    )
    args = parser.parse_args()

    repo = Path(__file__).resolve().parents[1]
    lua_path = (
        args.lua
        if args.lua.is_absolute()
        else Path(__file__).resolve().parent / args.lua
    )
    idx_path = args.idx_path or Path(str(args.tar_path) + ".utix")

    sys.path.insert(0, str(repo / "python" / "src"))
    try:
        from ultar_dataloader import DataLoader
    except ImportError as exc:
        raise SystemExit(
            "build/install the Python bindings first: zig build python-bindings"
        ) from exc

    loader = DataLoader(
        src=lua_path.read_text(),
        config={
            "tar_path": str(args.tar_path),
            "idx_path": str(idx_path),
            "max_rows": str(args.max_rows),
            "max_dimension": str(args.max_dimension),
        },
    )

    print(
        f"=== DataLoader demo: skip images with width/height > {args.max_dimension} ==="
    )
    for i, row in enumerate(loader):
        print(f"  row {i}: keys={row.keys()} jpg_bytes={len(row['.jpg'])}")


if __name__ == "__main__":
    main()
