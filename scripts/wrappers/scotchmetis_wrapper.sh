#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"

GRAPH=""
K=""
OUT=""
SCOTCHMETIS_BIN="${SCOTCHMETIS_BIN:-}"
GPART_BIN="${GPART_BIN:-}"
GCV_BIN="${GCV_BIN:-}"

usage() {
  cat <<'EOF'
Run Scotch METIS-compat binary and normalize output assignment path.

Usage:
  bash scripts/wrappers/scotchmetis_wrapper.sh --graph <path> --k <int> --out <path> [--scotchmetis-bin <path>]
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --graph)
      GRAPH="${2:?missing value for --graph}"
      shift 2
      ;;
    --k)
      K="${2:?missing value for --k}"
      shift 2
      ;;
    --out)
      OUT="${2:?missing value for --out}"
      shift 2
      ;;
    --scotchmetis-bin)
      SCOTCHMETIS_BIN="${2:?missing value for --scotchmetis-bin}"
      shift 2
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      echo "[ERROR] Unknown argument: $1"
      usage
      exit 1
      ;;
  esac
done

if [[ -z "$GRAPH" || -z "$K" || -z "$OUT" ]]; then
  usage
  exit 1
fi

if [[ ! "$K" =~ ^[0-9]+$ ]] || [[ "$K" -lt 2 ]]; then
  echo "[ERROR] --k must be an integer >= 2"
  exit 1
fi

if [[ ! -f "$GRAPH" ]]; then
  echo "[ERROR] Graph file not found: $GRAPH"
  exit 1
fi

if [[ -n "$SCOTCHMETIS_BIN" ]] && [[ "$(basename "$SCOTCHMETIS_BIN")" == "gpart" ]]; then
  GPART_BIN="$SCOTCHMETIS_BIN"
  SCOTCHMETIS_BIN=""
fi

if [[ -z "$SCOTCHMETIS_BIN" ]]; then
  if [[ -x "$PROJECT_DIR/algorithms/scotch/bin/scotchmetis" ]]; then
    SCOTCHMETIS_BIN="$PROJECT_DIR/algorithms/scotch/bin/scotchmetis"
  elif command -v scotchmetis >/dev/null 2>&1; then
    SCOTCHMETIS_BIN="$(command -v scotchmetis)"
  fi
fi

if [[ -n "$SCOTCHMETIS_BIN" ]]; then
  if [[ ! -x "$SCOTCHMETIS_BIN" ]]; then
    echo "[ERROR] scotchmetis is not executable: $SCOTCHMETIS_BIN"
    exit 1
  fi

  echo "[RUN] $SCOTCHMETIS_BIN $GRAPH $K"
  "$SCOTCHMETIS_BIN" "$GRAPH" "$K"

  default_part="${GRAPH}.part.${K}"
  cwd_part="$(pwd)/$(basename "$GRAPH").part.${K}"

  src_part=""
  if [[ -f "$default_part" ]]; then
    src_part="$default_part"
  elif [[ -f "$cwd_part" ]]; then
    src_part="$cwd_part"
  fi

  if [[ -z "$src_part" ]]; then
    echo "[ERROR] scotchmetis did not produce expected partition file."
    echo "        looked for: $default_part"
    echo "        and:       $cwd_part"
    exit 1
  fi

  mkdir -p "$(dirname "$OUT")"
  cp -f "$src_part" "$OUT"
  echo "[OK] Partition assignment copied to: $OUT"
  exit 0
fi

if [[ -z "$GPART_BIN" ]]; then
  if [[ -x "$PROJECT_DIR/algorithms/scotch/bin/gpart" ]]; then
    GPART_BIN="$PROJECT_DIR/algorithms/scotch/bin/gpart"
  elif [[ -x "$PROJECT_DIR/algorithms/scotch-7.0.4/src/scotch/gpart" ]]; then
    GPART_BIN="$PROJECT_DIR/algorithms/scotch-7.0.4/src/scotch/gpart"
  elif command -v gpart >/dev/null 2>&1; then
    GPART_BIN="$(command -v gpart)"
  else
    echo "[ERROR] Could not find scotchmetis or gpart binary."
    echo "        Try: provide --scotchmetis-bin <path> or install Scotch locally."
    echo "        Or pass --scotchmetis-bin <path>."
    exit 1
  fi
fi

if [[ -z "$GCV_BIN" ]]; then
  if [[ -x "$PROJECT_DIR/algorithms/scotch/bin/gcv" ]]; then
    GCV_BIN="$PROJECT_DIR/algorithms/scotch/bin/gcv"
  elif [[ -x "$PROJECT_DIR/algorithms/scotch-7.0.4/src/scotch/gcv" ]]; then
    GCV_BIN="$PROJECT_DIR/algorithms/scotch-7.0.4/src/scotch/gcv"
  elif command -v gcv >/dev/null 2>&1; then
    GCV_BIN="$(command -v gcv)"
  else
    echo "[ERROR] Could not find gcv binary required for gpart fallback."
    echo "        Try: provide --scotchmetis-bin <path> or install Scotch locally."
    exit 1
  fi
fi

if [[ ! -x "$GPART_BIN" ]]; then
  echo "[ERROR] gpart is not executable: $GPART_BIN"
  exit 1
fi
if [[ ! -x "$GCV_BIN" ]]; then
  echo "[ERROR] gcv is not executable: $GCV_BIN"
  exit 1
fi

tmp_dir="$(mktemp -d)"
trap 'rm -rf "$tmp_dir"' EXIT
graph_grf="$tmp_dir/graph.grf"
map_file="$tmp_dir/graph.map"

echo "[INFO] scotchmetis not found, using gpart fallback"
echo "[RUN] $GCV_BIN -ic -os $GRAPH $graph_grf"
"$GCV_BIN" -ic -os "$GRAPH" "$graph_grf"
echo "[RUN] $GPART_BIN $K $graph_grf $map_file"
"$GPART_BIN" "$K" "$graph_grf" "$map_file"

if [[ ! -f "$map_file" ]]; then
  echo "[ERROR] gpart did not produce mapping file: $map_file"
  exit 1
fi

mkdir -p "$(dirname "$OUT")"
awk '
NR == 1 { next }
NF >= 2 { print $1 "\t" $2 }
' "$map_file" \
| sort -n -k1,1 \
| awk '
{
  if ($1 != NR) {
    printf("[ERROR] Non-sequential node label in map file at line %d: %s\n", NR + 1, $1) > "/dev/stderr"
    exit 2
  }
  print $2
}
END {
  if (NR == 0) {
    print "[ERROR] Empty mapping output from gpart" > "/dev/stderr"
    exit 3
  }
}
' > "$OUT"

echo "[OK] Partition assignment copied to: $OUT"
