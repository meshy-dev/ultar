import gc
import io
import os
import subprocess
import sys
import tarfile
import textwrap
from dataclasses import dataclass
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[2]
PACKAGE_ROOT = REPO_ROOT / "python" / "src"
INDEXER = REPO_ROOT / "zig-out" / "bin" / "indexer"
LOADER_SCRIPT = Path(__file__).with_name("loader_script.lua")


@dataclass(frozen=True)
class GeneratedFixture:
    tar_path: Path
    index_path: Path


def _make_tar_fixture(root: Path) -> GeneratedFixture:
    tar_path = root / "generated.tar"
    rows = {
        "row0": {
            ".txt": b"first row text",
            ".json": b'{"row": 0}',
            ".bin": bytes([0, 1, 2, 3]),
        },
        "row1": {
            ".txt": b"second row text",
            ".json": b'{"row": 1}',
            ".bin": bytes([4, 5, 6, 7]),
        },
        "row2": {
            ".txt": b"third row text",
            ".json": b'{"row": 2}',
            ".bin": bytes([8, 9, 10, 11]),
        },
    }

    with tarfile.open(tar_path, "w") as archive:
        for row_name, entries in rows.items():
            for suffix, payload in entries.items():
                member = tarfile.TarInfo(f"{row_name}{suffix}")
                member.size = len(payload)
                archive.addfile(member, io.BytesIO(payload))

    subprocess.run([str(INDEXER), "-f", str(tar_path)], cwd=root, check=True)
    return GeneratedFixture(tar_path=tar_path, index_path=Path(f"{tar_path}.utix"))


def _pythonpath_env() -> dict[str, str]:
    env = os.environ.copy()
    path_parts = [str(PACKAGE_ROOT)]
    if current := env.get("PYTHONPATH"):
        path_parts.append(current)
    env["PYTHONPATH"] = os.pathsep.join(path_parts)
    return env


def _run_subprocess(
    case: str, generated_fixture: GeneratedFixture
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            sys.executable,
            "-c",
            textwrap.dedent(case),
            str(LOADER_SCRIPT),
            str(generated_fixture.tar_path),
            str(generated_fixture.index_path),
        ],
        cwd=REPO_ROOT,
        env=_pythonpath_env(),
        capture_output=True,
        text=True,
    )


def _assert_clean_exit(result: subprocess.CompletedProcess[str]) -> None:
    assert result.returncode == 0, (
        f"subprocess exited with {result.returncode}\n"
        f"stdout:\n{result.stdout}\n"
        f"stderr:\n{result.stderr}"
    )


@pytest.fixture(scope="module")
def generated_fixture(tmp_path_factory: pytest.TempPathFactory) -> GeneratedFixture:
    root = tmp_path_factory.mktemp("limited-abi")
    return _make_tar_fixture(root)


def test_subprocess_repeated_construction_and_cleanup(
    generated_fixture: GeneratedFixture,
) -> None:
    result = _run_subprocess(
        """
        import gc
        import sys
        from pathlib import Path

        from ultar_dataloader import DataLoader

        script = Path(sys.argv[1]).read_text()
        config = {
            "tar_path": sys.argv[2],
            "idx_path": sys.argv[3],
            "max_rows": "2",
        }

        for _ in range(40):
            loader = DataLoader(src=script, config=config)
            rows = list(loader)
            assert len(rows) == 2
            assert rows[0].keys() == [".txt", ".json", ".bin"]
            assert rows[0][".txt"] == b"first row text"
            assert rows[0].to_dict()[".json"] == b'{"row": 0}'
            del rows
            del loader
            for _ in range(3):
                gc.collect()
        """,
        generated_fixture,
    )

    _assert_clean_exit(result)


def test_subprocess_constructor_failure_cleanup(
    generated_fixture: GeneratedFixture,
) -> None:
    result = _run_subprocess(
        """
        import gc
        from ultar_dataloader import DataLoader

        for _ in range(40):
            try:
                DataLoader(src="this is not lua")
            except RuntimeError:
                pass
            else:
                raise AssertionError("expected RuntimeError for invalid Lua source")
            for _ in range(3):
                gc.collect()
        """,
        generated_fixture,
    )

    _assert_clean_exit(result)


def test_subprocess_row_error_paths_and_parent_release(
    generated_fixture: GeneratedFixture,
) -> None:
    result = _run_subprocess(
        """
        import gc
        import sys
        from pathlib import Path

        from ultar_dataloader import DataLoader

        script = Path(sys.argv[1]).read_text()
        config = {
            "tar_path": sys.argv[2],
            "idx_path": sys.argv[3],
            "max_rows": "1",
        }

        for _ in range(40):
            loader = DataLoader(src=script, config=config)
            row = next(iter(loader))
            del loader
            gc.collect()

            assert row[0] == b"first row text"
            assert row[-1] == bytes([0, 1, 2, 3])

            try:
                row[99]
            except IndexError:
                pass
            else:
                raise AssertionError("expected IndexError for out-of-range row access")

            try:
                row[".missing"]
            except KeyError:
                pass
            else:
                raise AssertionError("expected KeyError for missing row key")

            del row
            for _ in range(3):
                gc.collect()
        """,
        generated_fixture,
    )

    _assert_clean_exit(result)


def test_generated_fixture_reuse_is_stable(generated_fixture: GeneratedFixture) -> None:
    assert generated_fixture.tar_path.exists()
    assert generated_fixture.index_path.exists()
    gc.collect()
