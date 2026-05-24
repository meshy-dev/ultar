#!/usr/bin/env bash
# benchmark_imagenet_metadata.sh
# Downloads 8 shards of imagenet-12k-wds and benchmarks indexer
# performance with and without --meta-rule (running all 8 indexers concurrently).
#
# The script ALWAYS runs a fast synthetic smoke test first that:
#   - Builds a 2-sample WDS-like tar matching the real dataset schema
#     ({"label": N, "width": W, "height": H, "filename": "..."})
#   - Indexes it with the real meta-rule using --fmt jsonl
#   - Verifies the produced .utix contains a "metadata" map with the
#     correct dotted query keys (".label", ".width", ...) and values.
#   - Also verifies that a run *without* --meta-rule produces no "metadata" key.
#
# This guarantees we are extracting the *actual* JSON fields and that the
# .utix is parsable (both msgpack default and jsonl).
#
# Usage:
#   ./scripts/benchmark_imagenet_metadata.sh
#
# Environment overrides:
#   DATA_DIR     default /tmp/imagenet12k-wds
#   INDEXER      default ./zig-out/bin/indexer   (script will build if missing)
#   ZIG          default zig                      (used only if INDEXER is missing)
#   NUM_SHARDS   default 8
#   META_RULE    default '.json:.label;.width;.height;.filename'
#   SKIP_BIG=1   skip the 8-shard concurrent timing (only do the fast verify)
#
# Notes:
#   - Metadata sub-object keys are the *query strings* you passed (with the dot).
#     So row.metadata[".label"], row.metadata[".width"] etc. in Lua / DataLoader.
#   - The verify step uses only Python stdlib (json) + tar + the indexer.

set -euo pipefail

DATA_DIR="${DATA_DIR:-/tmp/imagenet12k-wds}"
INDEXER="${INDEXER:-./zig-out/bin/indexer}"
ZIG="${ZIG:-zig}"
NUM_SHARDS="${NUM_SHARDS:-8}"
BASE_URL="https://huggingface.co/datasets/dark-xet/imagenet-12k-wds/resolve/main"

# The rule that matches the real structure of the HF shards
META_RULE="${META_RULE:-.json:.label;.width;.height;.filename}"

mkdir -p "$DATA_DIR"

echo "=== Imagenet-12k WDS Metadata Rules Benchmark ==="
echo "Data dir   : $DATA_DIR"
echo "Indexer    : $INDEXER"
echo "Shards     : 0000..$(printf "%04d" $((NUM_SHARDS-1)))"
echo "Meta rule  : --meta-rule '$META_RULE'"
echo

# Ensure indexer exists (build if necessary)
if [ ! -x "$INDEXER" ]; then
    echo "Indexer not found – building (ReleaseSafe)..."
    (cd "$(dirname "$0")/.." && "$ZIG" build -Doptimize=ReleaseSafe --summary none)
    if [ ! -x "$INDEXER" ]; then
        echo "ERROR: build did not produce $INDEXER"
        exit 1
    fi
fi

# ------------------------------------------------------------------
# Fast self-verification using a synthetic 2-sample shard
# ------------------------------------------------------------------
verify_smoke() {
    echo "=== Fast smoke verify (synthetic WDS tar + real meta-rule) ==="

    local tdir
    tdir=$(mktemp -d /tmp/wds_smoke_XXXXXX)
    local shard="$tdir/shard.tar"
    local utix

    # Create two realistic samples (same schema as the real HF data)
    mkdir -p "$tdir/shard"
    (
        cd "$tdir/shard"
        cat > n03615563_10371.json <<'J'
{"label":4888,"width":375,"height":500,"filename":"n03615563_10371.JPEG"}
J
        printf '4888\n' > n03615563_10371.cls
        : > n03615563_10371.jpg

        cat > n02469248_2525.json <<'J'
{"label":123,"width":224,"height":224,"filename":"n02469248_2525.JPEG"}
J
        printf '123\n' > n02469248_2525.cls
        : > n02469248_2525.jpg
    )

    # Clean tar (no leading ./, deterministic order)
    tar -cf "$shard" --sort=name --transform='s,^,,' -C "$tdir/shard" \
        n03615563_10371.cls n03615563_10371.jpg n03615563_10371.json \
        n02469248_2525.cls n02469248_2525.jpg n02469248_2525.json

    # 1) Index WITH the meta rule (jsonl for easy inspection)
    utix="${shard}.utix"
    "$INDEXER" -f "$shard" --meta-rule "$META_RULE" --fmt jsonl > /dev/null 2>&1

    # Verify with pure Python (stdlib json)
    python3 -c '
import json, sys, os
utix = sys.argv[1]
with open(utix) as f:
    rows = [json.loads(l) for l in f if l.strip()]
print(f"  rows in utix: {len(rows)}")
all_ok = True
for i, r in enumerate(rows):
    meta = r.get("metadata")
    if not meta:
        print(f"  FAIL row {i}: no metadata key")
        all_ok = False
        continue
    expected = {".label", ".width", ".height", ".filename"}
    got = set(meta.keys())
    if got != expected:
        print(f"  FAIL row {i}: metadata keys {got} != {expected}")
        all_ok = False
    # spot-check values from the two samples
    if i == 0:
        if meta.get(".label") != 4888 or meta.get(".width") != 375:
            print("  FAIL row 0 values")
            all_ok = False
    if i == 1:
        if meta.get(".label") != 123 or meta.get(".width") != 224:
            print("  FAIL row 1 values")
            all_ok = False
if all_ok:
    print("  ✓ WITH meta-rule: metadata map present, correct keys & values, parsable jsonl")
else:
    print("  ✗ WITH meta-rule verification failed")
    sys.exit(1)
' "$utix"

    # 2) Also verify that a run WITHOUT the rule produces *no* "metadata" key
    rm -f "$utix"
    "$INDEXER" -f "$shard" --fmt jsonl > /dev/null 2>&1
    python3 -c '
import json, sys
utix = sys.argv[1]
with open(utix) as f:
    rows = [json.loads(l) for l in f if l.strip()]
for i, r in enumerate(rows):
    if "metadata" in r:
        print(f"  FAIL row {i}: metadata key present without --meta-rule")
        sys.exit(1)
print("  ✓ WITHOUT meta-rule: no \"metadata\" key (as expected)")
' "$utix"

    rm -rf "$tdir"
    echo "Smoke verify: PASSED"
    echo
}

# Always run the fast, self-contained verification first
if [[ "${SKIP_VERIFY:-}" != "1" ]]; then
    verify_smoke
else
    echo "(skipping smoke verify because SKIP_VERIFY=1)"
fi

if [[ "${SKIP_BIG:-}" == "1" ]]; then
    echo "SKIP_BIG=1 set – only smoke verify was executed."
    exit 0
fi

# ------------------------------------------------------------------
# Real benchmark on the 8 HF shards (concurrent)
# ------------------------------------------------------------------

# Download shards (sequential, resumable)
echo "Downloading shards (first run may take minutes)..."
for i in $(seq -f "%04g" 0 $((NUM_SHARDS-1))); do
    FILE="imagenet12k-train-${i}.tar"
    URL="${BASE_URL}/${FILE}"
    DEST="$DATA_DIR/$FILE"
    if [ -f "$DEST" ]; then
        echo "  $FILE already present ($(du -h "$DEST" | cut -f1))"
    else
        echo "  Downloading $FILE ..."
        curl -L -C - --progress-bar --fail -o "$DEST" "$URL"
    fi
done
echo "Downloads done."
echo

run_concurrent() {
    local label="$1"
    shift || true
    local extra_args=("$@")

    echo "=== Benchmark: $label ==="
    echo "Extra args: ${extra_args[*]:-none}"

    local start_ts end_ts wall
    start_ts=$(date +%s.%N)

    local pids=()
    for i in $(seq -f "%04g" 0 $((NUM_SHARDS-1))); do
        local tar="$DATA_DIR/imagenet12k-train-${i}.tar"
        local log="/tmp/indexer_${label}_${i}.log"

        "$INDEXER" -f "$tar" "${extra_args[@]}" > "$log" 2>&1 &
        pids+=($!)
    done

    local failed=0
    for pid in "${pids[@]}"; do
        if ! wait "$pid"; then
            failed=$((failed + 1))
        fi
    done

    end_ts=$(date +%s.%N)
    wall=$(python3 -c "print(f'{(float('$end_ts') - float('$start_ts')):.3f}')")

    echo "Wall-clock time: ${wall}s"
    echo "Failed processes: $failed"
    echo "First shard log tail:"
    tail -n 8 "/tmp/indexer_${label}_0000.log" 2>/dev/null || echo "  (no log)"
    echo
}

# Baseline – pure header scan (no content reads)
run_concurrent "no_meta_rule"

# With the real meta rule – exercises content read + std.json parsing for every .json member
run_concurrent "with_meta_rule" --meta-rule "$META_RULE"

echo "=== Comparison complete ==="
echo "Wall times above are for 8 concurrent indexer processes."
echo "Logs: /tmp/indexer_*.log"
echo
echo "Each shard now has a sibling .utix (msgpack) containing the metadata map."
echo "Example inspection (after a run):"
echo "  python -c '"
echo "  import json, subprocess"
echo "  # for jsonl version: head -1 shard.tar.utix | python -m json.tool"
echo "  # for real msgpack utix you can use the DataLoader or a small Zig/Python dumper"
echo "  '"
echo
echo "To clean downloads: rm -rf $DATA_DIR"
echo
echo "Tip: for production numbers use -Doptimize=ReleaseFast + fast NVMe."
