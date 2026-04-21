#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"

GRAPH=""
K=""
OUT=""
MPI=""
PTSCOTCHMETIS_BIN="${PTSCOTCHMETIS_BIN:-}"
MPIRUN_BIN="${MPIRUN_BIN:-}"
DGPART_BIN="${DGPART_BIN:-}"
GCV_BIN="${GCV_BIN:-}"

usage() {
  cat <<'EOF'
Run PT-Scotch METIS-compat binary via MPI and normalize output assignment path.

Usage:
  bash scripts/wrappers/ptscotchmetis_wrapper.sh \
    --graph <path> --k <int> --out <path> --mpi <int> \
    [--ptscotchmetis-bin <path>] [--mpirun-bin <path>]
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
    --mpi)
      MPI="${2:?missing value for --mpi}"
      shift 2
      ;;
    --ptscotchmetis-bin)
      PTSCOTCHMETIS_BIN="${2:?missing value for --ptscotchmetis-bin}"
      shift 2
      ;;
    --mpirun-bin)
      MPIRUN_BIN="${2:?missing value for --mpirun-bin}"
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

if [[ -z "$GRAPH" || -z "$K" || -z "$OUT" || -z "$MPI" ]]; then
  usage
  exit 1
fi

if [[ ! "$K" =~ ^[0-9]+$ ]] || [[ "$K" -lt 2 ]]; then
  echo "[ERROR] --k must be an integer >= 2"
  exit 1
fi

if [[ ! "$MPI" =~ ^[0-9]+$ ]] || [[ "$MPI" -lt 2 ]]; then
  echo "[ERROR] --mpi must be an integer >= 2 for PT-Scotch"
  exit 1
fi

if [[ ! -f "$GRAPH" ]]; then
  echo "[ERROR] Graph file not found: $GRAPH"
  exit 1
fi

if [[ -z "$MPIRUN_BIN" ]]; then
  if command -v mpirun >/dev/null 2>&1; then
    MPIRUN_BIN="$(command -v mpirun)"
  else
    echo "[ERROR] Could not find mpirun in PATH."
    echo "        Install OpenMPI/MPICH or pass --mpirun-bin <path>."
    exit 1
  fi
fi

if [[ ! -x "$MPIRUN_BIN" ]]; then
  echo "[ERROR] mpirun is not executable: $MPIRUN_BIN"
  exit 1
fi

if [[ -n "$PTSCOTCHMETIS_BIN" ]] && [[ "$(basename "$PTSCOTCHMETIS_BIN")" == "dgpart" ]]; then
  DGPART_BIN="$PTSCOTCHMETIS_BIN"
  PTSCOTCHMETIS_BIN=""
fi

if [[ -z "$PTSCOTCHMETIS_BIN" ]]; then
  if [[ -x "$PROJECT_DIR/algorithms/scotch/bin/ptscotchmetis" ]]; then
    PTSCOTCHMETIS_BIN="$PROJECT_DIR/algorithms/scotch/bin/ptscotchmetis"
  elif command -v ptscotchmetis >/dev/null 2>&1; then
    PTSCOTCHMETIS_BIN="$(command -v ptscotchmetis)"
  fi
fi

if [[ -n "$PTSCOTCHMETIS_BIN" ]]; then
  if [[ ! -x "$PTSCOTCHMETIS_BIN" ]]; then
    echo "[ERROR] ptscotchmetis is not executable: $PTSCOTCHMETIS_BIN"
    exit 1
  fi

  echo "[RUN] $MPIRUN_BIN -np $MPI $PTSCOTCHMETIS_BIN $GRAPH $K"
  "$MPIRUN_BIN" -np "$MPI" "$PTSCOTCHMETIS_BIN" "$GRAPH" "$K"

  default_part="${GRAPH}.part.${K}"
  cwd_part="$(pwd)/$(basename "$GRAPH").part.${K}"

  src_part=""
  if [[ -f "$default_part" ]]; then
    src_part="$default_part"
  elif [[ -f "$cwd_part" ]]; then
    src_part="$cwd_part"
  fi

  if [[ -z "$src_part" ]]; then
    echo "[ERROR] ptscotchmetis did not produce expected partition file."
    echo "        looked for: $default_part"
    echo "        and:       $cwd_part"
    exit 1
  fi

  mkdir -p "$(dirname "$OUT")"
  cp -f "$src_part" "$OUT"
  echo "[OK] Partition assignment copied to: $OUT"
  exit 0
fi

if [[ -z "$DGPART_BIN" ]]; then
  if [[ -x "$PROJECT_DIR/algorithms/scotch/bin/dgpart" ]]; then
    DGPART_BIN="$PROJECT_DIR/algorithms/scotch/bin/dgpart"
  elif [[ -x "$PROJECT_DIR/algorithms/scotch-7.0.4/src/scotch/dgpart" ]]; then
    DGPART_BIN="$PROJECT_DIR/algorithms/scotch-7.0.4/src/scotch/dgpart"
  elif command -v dgpart >/dev/null 2>&1; then
    DGPART_BIN="$(command -v dgpart)"
  else
    echo "[ERROR] Could not find ptscotchmetis or dgpart binary."
    echo "        Try: provide --ptscotchmetis-bin <path> or install PT-Scotch locally."
    echo "        Or pass --ptscotchmetis-bin <path>."
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
    echo "[ERROR] Could not find gcv binary required for dgpart fallback."
    echo "        Try: provide --ptscotchmetis-bin <path> or install PT-Scotch locally."
    exit 1
  fi
fi

if [[ ! -x "$DGPART_BIN" ]]; then
  echo "[ERROR] dgpart is not executable: $DGPART_BIN"
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

echo "[INFO] ptscotchmetis not found, using dgpart fallback"
echo "[RUN] $GCV_BIN -ic -os $GRAPH $graph_grf"
"$GCV_BIN" -ic -os "$GRAPH" "$graph_grf"
echo "[RUN] $MPIRUN_BIN -np $MPI $DGPART_BIN $K $graph_grf $map_file"
"$MPIRUN_BIN" -np "$MPI" "$DGPART_BIN" "$K" "$graph_grf" "$map_file"

if [[ ! -f "$map_file" ]]; then
  echo "[ERROR] dgpart did not produce mapping file: $map_file"
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
    print "[ERROR] Empty mapping output from dgpart" > "/dev/stderr"
    exit 3
  }
}
' > "$OUT"

echo "[OK] Partition assignment copied to: $OUT"
