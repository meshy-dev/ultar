#!/usr/bin/env python3
"""Benchmark ImageNet-12k WDS indexing with and without metadata.

This is intentionally written as a readable example:

1. Build the indexer if needed.
2. Download/cache 8 real ImageNet-12k tar shards.
3. Run 8 indexer processes concurrently without metadata.
4. Run 8 indexer processes concurrently with metadata.

Example:
    uv run python benchmark_imagenet_metadata.py

Useful environment variables:
    DATA_DIR=/tmp/imagenet12k-wds
    INDEXER=./zig-out/bin/indexer
    ZIG=zig
"""

from __future__ import annotations

import argparse
import os
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from huggingface_hub import hf_hub_download


HF_REPO_ID = "dark-xet/imagenet-12k-wds"
DEFAULT_META_RULE = ".json:.label;.width;.height;.filename"


def run(cmd: list[str], *, cwd: Path | None = None, stdout=None, stderr=None) -> None:
    print("  $", " ".join(cmd))
    subprocess.run(cmd, check=True, cwd=cwd, stdout=stdout, stderr=stderr)


def ensure_indexer(repo: Path, indexer: Path, zig: str) -> None:
    if indexer.exists() and os.access(indexer, os.X_OK):
        return

    print(f"Indexer not found at {indexer}; building it with {zig!r}...")
    run(
        [zig, "build", "-Doptimize=ReleaseSafe", "--summary", "none"],
        cwd=repo,
        stdout=subprocess.DEVNULL,
    )
    if not indexer.exists():
        raise SystemExit(f"build did not produce {indexer}")


def shard_name(i: int) -> str:
    return f"imagenet12k-train-{i:04d}.tar"


def download_shards(data_dir: Path, num_shards: int) -> list[Path]:
    print("=== Download/cache shards ===")
    data_dir.mkdir(parents=True, exist_ok=True)
    paths: list[Path] = []
    for i in range(num_shards):
        name = shard_name(i)
        path = data_dir / name
        if path.exists():
            print(f"  cached {name} ({path.stat().st_size / 1024**2:.1f} MiB)")
        else:
            print(f"  downloading {name} through Hugging Face Hub/Xet")
            path = Path(
                hf_hub_download(
                    repo_id=HF_REPO_ID,
                    repo_type="dataset",
                    filename=name,
                    local_dir=data_dir,
                )
            )
        paths.append(path)
    return paths


def index_one(indexer: Path, tar_path: Path, extra_args: list[str]) -> None:
    log = (
        Path("/tmp")
        / f"indexer-{tar_path.stem}-{'meta' if extra_args else 'plain'}.log"
    )
    with log.open("w") as out:
        subprocess.run(
            [str(indexer), *extra_args, str(tar_path)],
            check=True,
            stdout=out,
            stderr=out,
        )


def benchmark(
    indexer: Path, tars: list[Path], label: str, extra_args: list[str]
) -> float:
    print(f"=== Benchmark: {label} ===")
    start = time.perf_counter()
    with ThreadPoolExecutor(max_workers=len(tars)) as pool:
        list(pool.map(lambda p: index_one(indexer, p, extra_args), tars))
    elapsed = time.perf_counter() - start
    print(f"  indexed {len(tars)} shards concurrently in {elapsed:.3f}s")
    return elapsed


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=Path(os.environ.get("DATA_DIR", "/tmp/imagenet12k-wds")),
    )
    parser.add_argument(
        "--indexer",
        type=Path,
        default=Path(os.environ.get("INDEXER", "./zig-out/bin/indexer")),
    )
    parser.add_argument("--zig", default=os.environ.get("ZIG", "zig"))
    parser.add_argument(
        "--num-shards", type=int, default=int(os.environ.get("NUM_SHARDS", "8"))
    )
    parser.add_argument(
        "--meta-rule", default=os.environ.get("META_RULE", DEFAULT_META_RULE)
    )
    args = parser.parse_args()

    repo = Path(__file__).resolve().parents[1]
    indexer = args.indexer if args.indexer.is_absolute() else repo / args.indexer

    print("=== ImageNet-12k metadata benchmark ===")
    print(f"data dir : {args.data_dir}")
    print(f"indexer  : {indexer}")
    print(f"meta rule: {args.meta_rule}")

    ensure_indexer(repo, indexer, args.zig)

    tars = download_shards(args.data_dir, args.num_shards)
    plain = benchmark(indexer, tars, "without metadata", [])
    meta = benchmark(indexer, tars, "with metadata", ["--meta-rule", args.meta_rule])
    print(f"metadata overhead: {meta - plain:+.3f}s wall-clock")


if __name__ == "__main__":
    main()
