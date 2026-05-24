import io
import json
import subprocess
import tarfile
from pathlib import Path

from ultar_dataloader import DataLoader


REPO_ROOT = Path(__file__).resolve().parents[2]
INDEXER = REPO_ROOT / "zig-out" / "bin" / "indexer"
META_RULE = ".json:.label;.width;.height;.filename"


def make_metadata_tar(path: Path) -> None:
    samples = {
        "n03615563_10371": {
            "label": 4888,
            "width": 375,
            "height": 500,
            "filename": "n03615563_10371.JPEG",
        },
        "n02469248_2525": {
            "label": 123,
            "width": 224,
            "height": 224,
            "filename": "n02469248_2525.JPEG",
        },
    }
    with tarfile.open(path, "w") as tar:
        for stem, meta in samples.items():
            for suffix, data in (
                (".cls", f"{meta['label']}\n".encode()),
                (".jpg", b""),
                (".json", json.dumps(meta, separators=(",", ":")).encode()),
            ):
                member = tarfile.TarInfo(stem + suffix)
                member.size = len(data)
                tar.addfile(member, io.BytesIO(data))


def read_jsonl(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text().splitlines()]


def test_indexer_metadata_rule_emits_expected_jsonl(tmp_path: Path) -> None:
    tar_path = tmp_path / "metadata.tar"
    make_metadata_tar(tar_path)

    subprocess.run(
        [str(INDEXER), "--fmt", "jsonl", "--meta-rule", META_RULE, str(tar_path)],
        cwd=REPO_ROOT,
        check=True,
    )

    rows = read_jsonl(Path(f"{tar_path}.utix"))
    assert len(rows) == 2
    assert rows[0]["metadata"] == {
        ".label": 4888,
        ".width": 375,
        ".height": 500,
        ".filename": "n03615563_10371.JPEG",
    }
    assert rows[1]["metadata"] == {
        ".label": 123,
        ".width": 224,
        ".height": 224,
        ".filename": "n02469248_2525.JPEG",
    }


def test_indexer_without_metadata_rule_omits_metadata(tmp_path: Path) -> None:
    tar_path = tmp_path / "metadata.tar"
    make_metadata_tar(tar_path)

    subprocess.run(
        [str(INDEXER), "--fmt", "jsonl", str(tar_path)], cwd=REPO_ROOT, check=True
    )

    rows = read_jsonl(Path(f"{tar_path}.utix"))
    assert len(rows) == 2
    assert "metadata" not in rows[0]
    assert "metadata" not in rows[1]


def test_dataloader_reads_metadata_and_emits_it_with_add_entry_bytes(
    tmp_path: Path,
) -> None:
    tar_path = tmp_path / "metadata.tar"
    make_metadata_tar(tar_path)

    subprocess.run(
        [str(INDEXER), "--meta-rule", META_RULE, str(tar_path)],
        cwd=REPO_ROOT,
        check=True,
    )

    lua_script = r"""
local loader = require("ultar.loader")
local utix = require("ultar.utix")

return {
  init_ctx = function(rank, world_size, config)
    return { idx_path = config.idx_path }
  end,

  row_generator = function(ctx)
    local idx = utix.open(ctx.idx_path)
    for row in idx:iter() do
      local meta = row.metadata or {}
      local payload = table.concat({
        tostring(meta[".label"]),
        tostring(meta[".width"]),
        tostring(meta[".height"]),
        tostring(meta[".filename"]),
      }, "|") .. string.char(0) .. "tail"
      loader:add_entry_bytes(".metadata.txt", payload)
      loader:finish_row()
    end
  end,
}
"""

    loader = DataLoader(src=lua_script, config={"idx_path": f"{tar_path}.utix"})
    rows = list(loader)

    assert [row[".metadata.txt"] for row in rows] == [
        b"4888|375|500|n03615563_10371.JPEG\0tail",
        b"123|224|224|n02469248_2525.JPEG\0tail",
    ]
