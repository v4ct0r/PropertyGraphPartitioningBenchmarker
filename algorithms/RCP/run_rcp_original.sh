#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INPUT_DIR=""
NODE_INDEX=""
K=""
OUT_DIR=""
BUILD_DIR="${SCRIPT_DIR}/build"

usage() {
  cat <<'USAGE'
Usage:
  bash tools/RCP/run_rcp_original.sh --input-dir <dir> --node-index <path> --k <n> --out-dir <dir>
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --input-dir)
      INPUT_DIR="${2:?missing value for --input-dir}"
      shift 2
      ;;
    --node-index)
      NODE_INDEX="${2:?missing value for --node-index}"
      shift 2
      ;;
    --k)
      K="${2:?missing value for --k}"
      shift 2
      ;;
    --out-dir)
      OUT_DIR="${2:?missing value for --out-dir}"
      shift 2
      ;;
    --help)
      usage
      exit 0
      ;;
    *)
      echo "[RCP] Unknown argument: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

[[ -n "$INPUT_DIR" && -n "$K" && -n "$OUT_DIR" ]] || {
  usage >&2
  exit 1
}

mkdir -p "$BUILD_DIR" "$OUT_DIR"
WORK_DIR="$OUT_DIR/work"
mkdir -p "$WORK_DIR"

compile() {
  local src="$1"
  local out="$2"
  g++ -std=c++17 -O2 -Wall -Wextra -pedantic -I"$SCRIPT_DIR" "$SCRIPT_DIR/$src" -o "$out"
}

compile csv2txt.cpp "$BUILD_DIR/csv2txt"
compile graph_v4.cpp "$BUILD_DIR/graph_v4"
compile partition_v4.cpp "$BUILD_DIR/partition_v4"

CSV2TXT_CMD=("$BUILD_DIR/csv2txt" --input-dir "$INPUT_DIR" --work-dir "$WORK_DIR")
if [[ -n "$NODE_INDEX" ]]; then
  CSV2TXT_CMD+=(--node-index "$NODE_INDEX")
fi

"${CSV2TXT_CMD[@]}"
"$BUILD_DIR/graph_v4" --work-dir "$WORK_DIR"
"$BUILD_DIR/partition_v4" --work-dir "$WORK_DIR" --k "$K" --assignment-out "$OUT_DIR/rcp_partition_k${K}.txt" --metrics-out "$OUT_DIR/rcp_metrics.json"
