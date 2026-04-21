#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
WORKSPACE_DIR="$(cd "$PROJECT_DIR/.." && pwd)"

DATASET=""
DATASET_DIR=""
DATASET_LABEL=""
PREP_MODE="auto"
ALGORITHMS_CSV="kahip_fast,metis,scotch"
PREP_INPUT_DIR=""
RAW_XML=""
RAW_ZIP_DIR=""
KS_CSV="2,4,6,8"
MPI_PROCS=4
RESULTS_ROOT="$PROJECT_DIR/results"
RETRIES=1
CONTINUE_ON_ERROR=1
MAX_RECORDS=0
SEED=42
STRICT_NATIVE=0
MATERIALIZE_PROPERTY_GRAPH=0
MATERIALIZE_EDGE_MODE="source"

METIS_BIN=""
PARMETIS_BIN=""
KAHIP_BIN=""
SCOTCHMETIS_BIN=""
PTSCOTCHMETIS_BIN=""
MPIRUN_BIN=""
SCOTCH_CMD=""
PTSCOTCH_CMD=""

PREP_NEO4J_SCRIPT="$PROJECT_DIR/scripts/prepare_neo4j_csv_for_partitioning.py"
PREP_DBLP_SCRIPT="$PROJECT_DIR/scripts/prepare_dblp_for_partitioning.py"
PREP_CORDIS_SCRIPT="$PROJECT_DIR/scripts/prepare_cordis_horizon_for_partitioning.py"
PREP_CORDIS_RCP_SCRIPT="$PROJECT_DIR/scripts/prepare_cordis_horizon_for_rcp.py"
MATERIALIZE_SCRIPT="$PROJECT_DIR/scripts/materialize_partitioned_property_graph.py"
RCP_RUNNER="$PROJECT_DIR/algorithms/RCP/run_rcp_original.sh"

usage() {
  cat <<'EOF'
General standalone runner for all partition algorithms across supported dataset types.

Algorithms:
  kahip_fast, metis, parmetis, scotch, ptscotch, rcp

Supported prep modes:
  auto            Detect from existing prepared files or known raw sources
  prepared        Use existing partition_prep outputs
  neo4j_csv       Prepare from a dataset directory of Neo4j-style CSV files
  dblp_xml        Prepare from DBLP XML
  cordis_horizon  Prepare from CORDIS Horizon zip files

Usage:
  bash scripts/run_all_algorithms.sh [options]

Core options:
  --dataset <name-or-path>   Dataset name under datasets or explicit path
  --dataset-dir <path>       Explicit dataset dir (alternative to --dataset)
  --dataset-label <txt>      Label used in result folder names
  --prep-mode <mode>         auto|prepared|neo4j_csv|dblp_xml|cordis_horizon (default: auto)
  --algorithms <csv>         Comma-separated algorithms (default: kahip_fast,metis,scotch)
  --prep-input-dir <path>    Explicit input directory for neo4j_csv prep
  --ks <csv>                 Comma-separated k values (default: 2,4,6,8)
  --mpi-procs <n>            MPI processes for PT-Scotch/ParMETIS (default: 4)
  --results-root <path>      Results root (default: results)
  --retries <n>              Retries per phase (default: 1)
  --seed <n>                 Seed for KaHIP/randomized components (default: 42)
  --stop-on-error            Stop immediately on first failure

Prep-specific options:
  --raw-xml <path>           Raw DBLP XML path for dblp_xml mode
  --raw-zip-dir <path>       Raw CORDIS zip folder for cordis_horizon mode
  --max-records <n>          Optional DBLP prep cap for testing (default: 0 = full)

Binary overrides:
  --metis-bin <path>         Path to gpmetis
  --parmetis-bin <path>      Path to parmetis (or pm_parmetis)
  --kahip-bin <path>         Path to kaffpa
  --scotchmetis-bin <path>   Path to scotchmetis or gpart
  --ptscotchmetis-bin <path> Path to ptscotchmetis or dgpart
  --mpirun-bin <path>        Path to mpirun

Command template overrides:
  --scotch-cmd <template>    Optional custom Scotch template with {graph} {k} {out} {mpi}
  --ptscotch-cmd <template>  Optional custom PT-Scotch template with {graph} {k} {out} {mpi}
  --strict-native            Disable implicit Scotch/PT-Scotch fallback search; only explicit overrides or exact binaries are allowed
  --materialize-property-graph
                             After each algorithm run, rebuild partitioned CSV folders from the original property-graph CSVs
  --materialize-edge-mode <mode>
                             source|duplicate-cross (default: source)
  --help                     Show this help

Examples:
  bash scripts/run_all_algorithms.sh --dataset fib25_neo4j_inputs
  bash scripts/run_all_algorithms.sh --dataset dblp_inputs --prep-mode prepared
  bash scripts/run_all_algorithms.sh --dataset dblp_inputs --prep-mode dblp_xml
  bash scripts/run_all_algorithms.sh --dataset cordis_horizon_inputs --prep-mode cordis_horizon
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dataset)
      DATASET="${2:?missing value for --dataset}"
      shift 2
      ;;
    --dataset-dir)
      DATASET_DIR="${2:?missing value for --dataset-dir}"
      shift 2
      ;;
    --dataset-label)
      DATASET_LABEL="${2:?missing value for --dataset-label}"
      shift 2
      ;;
    --prep-mode)
      PREP_MODE="${2:?missing value for --prep-mode}"
      shift 2
      ;;
    --algorithms)
      ALGORITHMS_CSV="${2:?missing value for --algorithms}"
      shift 2
      ;;
    --prep-input-dir)
      PREP_INPUT_DIR="${2:?missing value for --prep-input-dir}"
      shift 2
      ;;
    --raw-xml)
      RAW_XML="${2:?missing value for --raw-xml}"
      shift 2
      ;;
    --raw-zip-dir)
      RAW_ZIP_DIR="${2:?missing value for --raw-zip-dir}"
      shift 2
      ;;
    --ks)
      KS_CSV="${2:?missing value for --ks}"
      shift 2
      ;;
    --mpi-procs)
      MPI_PROCS="${2:?missing value for --mpi-procs}"
      shift 2
      ;;
    --results-root)
      RESULTS_ROOT="${2:?missing value for --results-root}"
      shift 2
      ;;
    --retries)
      RETRIES="${2:?missing value for --retries}"
      shift 2
      ;;
    --max-records)
      MAX_RECORDS="${2:?missing value for --max-records}"
      shift 2
      ;;
    --seed)
      SEED="${2:?missing value for --seed}"
      shift 2
      ;;
    --metis-bin)
      METIS_BIN="${2:?missing value for --metis-bin}"
      shift 2
      ;;
    --parmetis-bin)
      PARMETIS_BIN="${2:?missing value for --parmetis-bin}"
      shift 2
      ;;
    --kahip-bin)
      KAHIP_BIN="${2:?missing value for --kahip-bin}"
      shift 2
      ;;
    --scotchmetis-bin)
      SCOTCHMETIS_BIN="${2:?missing value for --scotchmetis-bin}"
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
    --scotch-cmd)
      SCOTCH_CMD="${2:?missing value for --scotch-cmd}"
      shift 2
      ;;
    --ptscotch-cmd)
      PTSCOTCH_CMD="${2:?missing value for --ptscotch-cmd}"
      shift 2
      ;;
    --stop-on-error)
      CONTINUE_ON_ERROR=0
      shift
      ;;
    --strict-native)
      STRICT_NATIVE=1
      shift
      ;;
    --materialize-property-graph)
      MATERIALIZE_PROPERTY_GRAPH=1
      shift
      ;;
    --materialize-edge-mode)
      MATERIALIZE_EDGE_MODE="${2:?missing value for --materialize-edge-mode}"
      shift 2
      ;;
    --help)
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

if [[ -z "$DATASET" && -z "$DATASET_DIR" ]]; then
  echo "[ERROR] Provide --dataset or --dataset-dir"
  exit 1
fi
if [[ ! "$MPI_PROCS" =~ ^[0-9]+$ ]] || [[ "$MPI_PROCS" -lt 2 ]]; then
  echo "[ERROR] --mpi-procs must be an integer >= 2"
  exit 1
fi
if [[ ! "$RETRIES" =~ ^[0-9]+$ ]]; then
  echo "[ERROR] --retries must be an integer >= 0"
  exit 1
fi
if [[ ! "$MAX_RECORDS" =~ ^[0-9]+$ ]]; then
  echo "[ERROR] --max-records must be an integer >= 0"
  exit 1
fi
if [[ ! "$SEED" =~ ^[0-9]+$ ]]; then
  echo "[ERROR] --seed must be an integer >= 0"
  exit 1
fi
if [[ ! "$PREP_MODE" =~ ^(auto|prepared|neo4j_csv|dblp_xml|cordis_horizon)$ ]]; then
  echo "[ERROR] Invalid --prep-mode: $PREP_MODE"
  exit 1
fi
if [[ ! "$MATERIALIZE_EDGE_MODE" =~ ^(source|duplicate-cross)$ ]]; then
  echo "[ERROR] Invalid --materialize-edge-mode: $MATERIALIZE_EDGE_MODE"
  exit 1
fi

IFS=',' read -r -a ALGORITHM_ARRAY <<< "$ALGORITHMS_CSV"
SELECTED_ALGORITHMS=()
for raw_algorithm in "${ALGORITHM_ARRAY[@]}"; do
  algorithm_name="$(echo "$raw_algorithm" | xargs)"
  if [[ "$algorithm_name" == "parallel_metis" ]]; then
    algorithm_name="parmetis"
  fi
  case "$algorithm_name" in
    kahip_fast|metis|parmetis|scotch|ptscotch|rcp)
      SELECTED_ALGORITHMS+=("$algorithm_name")
      ;;
    "")
      ;;
    *)
      echo "[ERROR] Invalid algorithm in --algorithms: $algorithm_name"
      exit 1
      ;;
  esac
done
if [[ "${#SELECTED_ALGORITHMS[@]}" -eq 0 ]]; then
  echo "[ERROR] --algorithms produced an empty selection"
  exit 1
fi

resolve_dataset() {
  if [[ -n "$DATASET_DIR" ]]; then
    DATASET_DIR="$(realpath -m "$DATASET_DIR")"
  elif [[ "$DATASET" = /* || "$DATASET" == ./* || "$DATASET" == ../* ]]; then
    DATASET_DIR="$(realpath -m "$DATASET")"
  else
    DATASET_DIR="$PROJECT_DIR/datasets/$DATASET"
  fi

  if [[ -z "$DATASET_LABEL" ]]; then
    if [[ -n "$DATASET" && "$DATASET" != /* && "$DATASET" != ./* && "$DATASET" != ../* ]]; then
      DATASET_LABEL="$DATASET"
    else
      DATASET_LABEL="$(basename "$DATASET_DIR")"
    fi
  fi
}

detect_prep_mode() {
  local prep_dir="$DATASET_DIR/partition_prep"
  local default_dblp_xml="$PROJECT_DIR/datasets/external_raw/dblp/dblp.xml"
  local default_cordis_zip_dir="$PROJECT_DIR/datasets/external_raw/cordis_horizon"

  if [[ "$PREP_MODE" != "auto" ]]; then
    return 0
  fi

  if [[ -f "$prep_dir/work/graph.metis" && -f "$prep_dir/node_index.tsv" && -f "$prep_dir/hash_nodes.txt" && -d "$prep_dir/hash_relationships_dummy" ]]; then
    PREP_MODE="prepared"
    return 0
  fi

  if [[ -z "$RAW_XML" && "$DATASET_LABEL" == "dblp_inputs" && -f "$default_dblp_xml" ]]; then
    RAW_XML="$default_dblp_xml"
    PREP_MODE="dblp_xml"
    return 0
  fi
  if [[ -n "$RAW_XML" ]]; then
    PREP_MODE="dblp_xml"
    return 0
  fi

  if [[ -z "$RAW_ZIP_DIR" && "$DATASET_LABEL" == "cordis_horizon_inputs" && -d "$default_cordis_zip_dir" ]]; then
    RAW_ZIP_DIR="$default_cordis_zip_dir"
    PREP_MODE="cordis_horizon"
    return 0
  fi
  if [[ -n "$RAW_ZIP_DIR" ]]; then
    PREP_MODE="cordis_horizon"
    return 0
  fi

  PREP_MODE="neo4j_csv"
}

resolve_dataset
detect_prep_mode

PREP_DIR="$DATASET_DIR/partition_prep"
GRAPH_FILE="$PREP_DIR/work/graph.metis"
NODE_INDEX_FILE="$PREP_DIR/node_index.tsv"
HASH_NODES_FILE="$PREP_DIR/hash_nodes.txt"
HASH_REL_DIR="$PREP_DIR/hash_relationships_dummy"

if [[ -z "$PREP_INPUT_DIR" ]]; then
  if [[ -d "$DATASET_DIR/combined" ]]; then
    PREP_INPUT_DIR="$DATASET_DIR/combined"
  else
    PREP_INPUT_DIR="$DATASET_DIR"
  fi
fi

mkdir -p "$RESULTS_ROOT/logs"

RUN_TAG="$(date +%Y%m%d_%H%M%S)"
MASTER_LOG="$RESULTS_ROOT/logs/run_all_algorithms_${DATASET_LABEL}_${RUN_TAG}.log"
SUMMARY_CSV="$RESULTS_ROOT/comparison_summary.csv"
SUMMARY_XLSX="$RESULTS_ROOT/comparison_summary.xlsx"
RCP_SUMMARY_CSV="$RESULTS_ROOT/rcp_comparison_summary.csv"
RCP_SUMMARY_XLSX="$RESULTS_ROOT/rcp_comparison_summary.xlsx"
ARTIFACTS_ROOT="$RESULTS_ROOT/algorithm_outputs"

echo "[INFO] start=$RUN_TAG" | tee -a "$MASTER_LOG"
echo "[INFO] dataset-dir=$DATASET_DIR" | tee -a "$MASTER_LOG"
echo "[INFO] prep-input-dir=$PREP_INPUT_DIR" | tee -a "$MASTER_LOG"
echo "[INFO] dataset-label=$DATASET_LABEL" | tee -a "$MASTER_LOG"
echo "[INFO] prep-mode=$PREP_MODE" | tee -a "$MASTER_LOG"
echo "[INFO] algorithms=${SELECTED_ALGORITHMS[*]}" | tee -a "$MASTER_LOG"
echo "[INFO] ks=$KS_CSV" | tee -a "$MASTER_LOG"
echo "[INFO] mpi-procs=$MPI_PROCS" | tee -a "$MASTER_LOG"
echo "[INFO] results-root=$RESULTS_ROOT" | tee -a "$MASTER_LOG"
echo "[INFO] artifacts-root=$ARTIFACTS_ROOT" | tee -a "$MASTER_LOG"
echo "[INFO] retries=$RETRIES" | tee -a "$MASTER_LOG"
echo "[INFO] continue-on-error=$CONTINUE_ON_ERROR" | tee -a "$MASTER_LOG"
echo "[INFO] strict-native=$STRICT_NATIVE" | tee -a "$MASTER_LOG"
echo "[INFO] materialize-property-graph=$MATERIALIZE_PROPERTY_GRAPH" | tee -a "$MASTER_LOG"
echo "[INFO] materialize-edge-mode=$MATERIALIZE_EDGE_MODE" | tee -a "$MASTER_LOG"

choose_binary_optional() {
  local override_path="$1"
  shift
  local -a defaults=("$@")

  if [[ -n "$override_path" ]]; then
    if [[ -x "$override_path" ]]; then
      printf '%s\n' "$override_path"
      return 0
    fi
    echo "[ERROR] Binary override is not executable: $override_path" >&2
    return 1
  fi

  local candidate
  for candidate in "${defaults[@]}"; do
    if [[ -x "$candidate" ]]; then
      printf '%s\n' "$candidate"
      return 0
    fi
  done

  local by_name
  by_name="$(basename "${defaults[0]}")"
  if command -v "$by_name" >/dev/null 2>&1; then
    command -v "$by_name"
    return 0
  fi

  return 1
}

fill_template() {
  local template="$1"
  local graph="$2"
  local k="$3"
  local out="$4"
  local mpi="$5"
  local rendered="$template"
  rendered="${rendered//\{graph\}/$graph}"
  rendered="${rendered//\{k\}/$k}"
  rendered="${rendered//\{out\}/$out}"
  rendered="${rendered//\{mpi\}/$mpi}"
  rendered="${rendered//\{output_root\}/$RESULTS_ROOT}"
  printf '%s\n' "$rendered"
}

copy_node_index() {
  local src="$1"
  local dest="$2"
  mkdir -p "$(dirname "$dest")"
  cp -f "$src" "$dest"
}

run_logged() {
  local log_file="$1"
  shift
  mkdir -p "$(dirname "$log_file")"
  "$@" 2>&1 | tee "$log_file"
}

run_logged_shell() {
  local log_file="$1"
  local cmd="$2"
  mkdir -p "$(dirname "$log_file")"
  /bin/bash -lc "$cmd" 2>&1 | tee "$log_file"
}

run_logged_append() {
  local log_file="$1"
  shift
  mkdir -p "$(dirname "$log_file")"
  "$@" 2>&1 | tee -a "$log_file"
}

run_logged_shell_append() {
  local log_file="$1"
  local cmd="$2"
  mkdir -p "$(dirname "$log_file")"
  /bin/bash -lc "$cmd" 2>&1 | tee -a "$log_file"
}

run_scotch_default() {
  local graph="$1"
  local k="$2"
  local out="$3"

  local gpart_bin=""
  local gcv_bin=""
  local scotchmetis_bin=""

  if [[ -n "$SCOTCHMETIS_BIN" && "$(basename "$SCOTCHMETIS_BIN")" == "gpart" ]]; then
    gpart_bin="$SCOTCHMETIS_BIN"
  else
    scotchmetis_bin="$(choose_binary_optional "$SCOTCHMETIS_BIN" \
      "$PROJECT_DIR/algorithms/scotch/bin/scotchmetis" \
      "$PROJECT_DIR/algorithms/scotch-7.0.4/src/scotch/scotchmetis")" || true
  fi

  if [[ -n "$scotchmetis_bin" ]]; then
    echo "[RUN] $scotchmetis_bin $graph $k"
    "$scotchmetis_bin" "$graph" "$k"
    local default_part="${graph}.part.${k}"
    local cwd_part="$(pwd)/$(basename "$graph").part.${k}"
    local src_part=""
    if [[ -f "$default_part" ]]; then
      src_part="$default_part"
    elif [[ -f "$cwd_part" ]]; then
      src_part="$cwd_part"
    else
      echo "[ERROR] scotchmetis did not produce expected partition file."
      return 1
    fi
    mkdir -p "$(dirname "$out")"
    cp -f "$src_part" "$out"
    echo "[OK] Partition assignment copied to: $out"
    return 0
  fi

  if [[ -z "$gpart_bin" ]]; then
    if [[ "$STRICT_NATIVE" -eq 1 ]]; then
      echo "[ERROR] Strict native mode: explicit --scotchmetis-bin must point to gpart, or scotchmetis must be available."
      return 1
    fi
    gpart_bin="$(choose_binary_optional "" \
      "$PROJECT_DIR/algorithms/scotch/bin/gpart" \
      "$PROJECT_DIR/algorithms/scotch-7.0.4/src/scotch/gpart")" || true
  fi
  if [[ "$STRICT_NATIVE" -eq 1 && -n "$SCOTCHMETIS_BIN" && "$(basename "$SCOTCHMETIS_BIN")" == "gpart" ]]; then
    gcv_bin="$(choose_binary_optional "" gcv /usr/bin/gcv)" || true
  else
    gcv_bin="$(choose_binary_optional "" \
    "$PROJECT_DIR/algorithms/scotch/bin/gcv" \
    "$PROJECT_DIR/algorithms/scotch-7.0.4/src/scotch/gcv")" || true
  fi

  if [[ -z "$gpart_bin" || -z "$gcv_bin" ]]; then
    echo "[ERROR] Could not find Scotch fallback binaries (gpart/gcv)."
    return 1
  fi

  local tmp_dir
  tmp_dir="$(mktemp -d)"
  trap 'rm -rf "$tmp_dir"' RETURN
  local graph_grf="$tmp_dir/graph.grf"
  local map_file="$tmp_dir/graph.map"

  echo "[INFO] scotchmetis not found, using gpart fallback"
  echo "[RUN] $gcv_bin -ic -os $graph $graph_grf"
  "$gcv_bin" -ic -os "$graph" "$graph_grf"
  echo "[RUN] $gpart_bin $k $graph_grf $map_file"
  "$gpart_bin" "$k" "$graph_grf" "$map_file"

  mkdir -p "$(dirname "$out")"
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
' > "$out"
  echo "[OK] Partition assignment copied to: $out"
}

run_ptscotch_default() {
  local graph="$1"
  local k="$2"
  local out="$3"
  local mpi="$4"

  local dgpart_bin=""
  local gcv_bin=""
  local ptscotchmetis_bin=""
  local mpirun_bin=""

  if [[ -n "$PTSCOTCHMETIS_BIN" && "$(basename "$PTSCOTCHMETIS_BIN")" == "dgpart" ]]; then
    dgpart_bin="$PTSCOTCHMETIS_BIN"
  else
    ptscotchmetis_bin="$(choose_binary_optional "$PTSCOTCHMETIS_BIN" \
      "$PROJECT_DIR/algorithms/scotch/bin/ptscotchmetis" \
      "$PROJECT_DIR/algorithms/scotch-7.0.4/src/scotch/ptscotchmetis")" || true
  fi

  mpirun_bin="$(choose_binary_optional "$MPIRUN_BIN" /usr/bin/mpirun mpirun)" || true
  if [[ -z "$mpirun_bin" ]]; then
    echo "[ERROR] Could not find mpirun."
    return 1
  fi

  if [[ -n "$ptscotchmetis_bin" ]]; then
    echo "[RUN] $mpirun_bin --oversubscribe -np $mpi $ptscotchmetis_bin $graph $k"
    "$mpirun_bin" --oversubscribe -np "$mpi" "$ptscotchmetis_bin" "$graph" "$k"
    local default_part="${graph}.part.${k}"
    local cwd_part="$(pwd)/$(basename "$graph").part.${k}"
    local src_part=""
    if [[ -f "$default_part" ]]; then
      src_part="$default_part"
    elif [[ -f "$cwd_part" ]]; then
      src_part="$cwd_part"
    else
      echo "[ERROR] ptscotchmetis did not produce expected partition file."
      return 1
    fi
    mkdir -p "$(dirname "$out")"
    cp -f "$src_part" "$out"
    echo "[OK] Partition assignment copied to: $out"
    return 0
  fi

  if [[ -z "$dgpart_bin" ]]; then
    if [[ "$STRICT_NATIVE" -eq 1 ]]; then
      echo "[ERROR] Strict native mode: explicit --ptscotchmetis-bin must point to dgpart, or ptscotchmetis must be available."
      return 1
    fi
    dgpart_bin="$(choose_binary_optional "" \
      "$PROJECT_DIR/algorithms/scotch/bin/dgpart" \
      "$PROJECT_DIR/algorithms/scotch-7.0.4/src/scotch/dgpart")" || true
  fi
  if [[ "$STRICT_NATIVE" -eq 1 && -n "$PTSCOTCHMETIS_BIN" && "$(basename "$PTSCOTCHMETIS_BIN")" == "dgpart" ]]; then
    gcv_bin="$(choose_binary_optional "" gcv /usr/bin/gcv)" || true
  else
    gcv_bin="$(choose_binary_optional "" \
    "$PROJECT_DIR/algorithms/scotch/bin/gcv" \
    "$PROJECT_DIR/algorithms/scotch-7.0.4/src/scotch/gcv")" || true
  fi

  if [[ -z "$dgpart_bin" || -z "$gcv_bin" ]]; then
    echo "[ERROR] Could not find PT-Scotch fallback binaries (dgpart/gcv)."
    return 1
  fi

  local tmp_dir
  tmp_dir="$(mktemp -d)"
  trap 'rm -rf "$tmp_dir"' RETURN
  local graph_grf="$tmp_dir/graph.grf"
  local map_file="$tmp_dir/graph.map"

  echo "[INFO] ptscotchmetis not found, using dgpart fallback"
  echo "[RUN] $gcv_bin -ic -os $graph $graph_grf"
  "$gcv_bin" -ic -os "$graph" "$graph_grf"
  echo "[RUN] $mpirun_bin --oversubscribe -np $mpi $dgpart_bin $k $graph_grf $map_file"
  "$mpirun_bin" --oversubscribe -np "$mpi" "$dgpart_bin" "$k" "$graph_grf" "$map_file"

  mkdir -p "$(dirname "$out")"
  if [[ ! -f "$map_file" ]]; then
    echo "[ERROR] PT-Scotch map output missing: $map_file"
    return 1
  fi

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
' > "$out"
  if [[ ! -s "$out" ]]; then
    echo "[ERROR] PT-Scotch partition output missing or empty: $out"
    return 1
  fi
  echo "[OK] Partition assignment copied to: $out"
}

run_parmetis_default() {
  local graph="$1"
  local k="$2"
  local out="$3"
  local mpi="$4"

  local parmetis_bin=""
  local mpirun_bin=""

  parmetis_bin="$(choose_binary_optional "$PARMETIS_BIN" \
    "$PROJECT_DIR/algorithms/parmetis/bin/parmetis" \
    "$PROJECT_DIR/algorithms/parmetis/bin/pm_parmetis" \
    "$PROJECT_DIR/algorithms/ParMETIS/build/Linux-x86_64/programs/parmetis" \
    "$PROJECT_DIR/algorithms/ParMETIS/build/Linux-x86_64/programs/pm_parmetis")" || true
  if [[ -z "$parmetis_bin" ]]; then
    echo "[ERROR] Could not find parmetis. Use --parmetis-bin."
    return 1
  fi

  mpirun_bin="$(choose_binary_optional "$MPIRUN_BIN" /usr/bin/mpirun mpirun)" || true
  if [[ -z "$mpirun_bin" ]]; then
    echo "[ERROR] Could not find mpirun."
    return 1
  fi

  local parmetis_parent
  local ld_paths=()
  parmetis_parent="$(dirname "$(dirname "$parmetis_bin")")"
  if [[ -d "$parmetis_parent/lib" ]]; then
    ld_paths+=("$parmetis_parent/lib")
  fi
  if [[ -d "$PROJECT_DIR/algorithms/parmetis/lib" ]]; then
    ld_paths+=("$PROJECT_DIR/algorithms/parmetis/lib")
  fi
  if [[ -d "$PROJECT_DIR/algorithms/parmetis/metis/lib" ]]; then
    ld_paths+=("$PROJECT_DIR/algorithms/parmetis/metis/lib")
  fi
  if [[ -d "$PROJECT_DIR/algorithms/parmetis/gklib/lib" ]]; then
    ld_paths+=("$PROJECT_DIR/algorithms/parmetis/gklib/lib")
  fi

  local tmp_part
  tmp_part="${graph}.part"
  rm -f "$tmp_part"
  echo "[RUN] $mpirun_bin --oversubscribe -np $mpi $parmetis_bin $graph 1 $k 1000 1000.0 0 $SEED"
  if [[ "${#ld_paths[@]}" -gt 0 ]]; then
    local joined
    joined="$(IFS=:; echo "${ld_paths[*]}")"
    /bin/bash -lc "export LD_LIBRARY_PATH='$joined':\${LD_LIBRARY_PATH:-}; '$mpirun_bin' --oversubscribe -np '$mpi' '$parmetis_bin' '$graph' 1 '$k' 1000 1000.0 0 '$SEED'"
  else
    "$mpirun_bin" --oversubscribe -np "$mpi" "$parmetis_bin" "$graph" 1 "$k" 1000 1000.0 0 "$SEED"
  fi

  if [[ ! -f "$tmp_part" ]]; then
    echo "[ERROR] ParMETIS output missing: $tmp_part"
    return 1
  fi
  mkdir -p "$(dirname "$out")"
  cp -f "$tmp_part" "$out"
  echo "[OK] Partition assignment copied to: $out"
}

update_summary() {
  local run_label="$1"
  local algorithm="$2"
  local k="$3"
  local log_file="$4"
  local part_file="$5"
  local graph_file="$6"
  local node_index_file="$7"
  local prepare_cmd="$8"
  local run_cmd="$9"
  local notes_override="${10}"
  local wall_time="${11}"

  python3 - "$run_label" "$algorithm" "$k" "$log_file" "$part_file" "$graph_file" "$node_index_file" "$prepare_cmd" "$run_cmd" "$notes_override" "$wall_time" "$SUMMARY_CSV" "$SUMMARY_XLSX" <<'PY'
import csv
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path

run_label = sys.argv[1]
algorithm = sys.argv[2]
k = sys.argv[3]
log_file = Path(sys.argv[4])
part_file = Path(sys.argv[5])
graph_file = Path(sys.argv[6])
node_index_file = Path(sys.argv[7])
prepare_cmd = sys.argv[8]
run_cmd = sys.argv[9]
notes_override = sys.argv[10]
wall_time = sys.argv[11]
summary_csv = Path(sys.argv[12])
summary_xlsx = Path(sys.argv[13])

text = log_file.read_text(encoding="utf-8", errors="ignore") if log_file.exists() else ""

def grab(pattern):
    m = re.findall(pattern, text, re.M)
    return m[-1] if m else ""

def parse_graph_header(path):
    with path.open("r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            s = line.strip()
            if not s or s.startswith("%"):
                continue
            parts = s.split()
            if len(parts) < 2:
                raise RuntimeError(f"Invalid METIS header: {path}")
            return int(parts[0]), int(parts[1]), parts[2] if len(parts) >= 3 else "0"
    raise RuntimeError(f"Missing METIS header: {path}")

def read_assignments(path):
    vals = []
    with path.open("r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            s = line.strip()
            if s:
                vals.append(int(s))
    return vals

def compute_metrics(graph_path, assignments):
    n, m, fmt = parse_graph_header(graph_path)
    if len(assignments) != n:
        raise RuntimeError(f"Assignment length mismatch: {len(assignments)} != {n}")
    edge_weights = fmt.endswith("1")
    cut = 0
    boundary = bytearray(n)
    part_comm = defaultdict(int)
    with graph_path.open("r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            s = line.strip()
            if s and not s.startswith("%"):
                break
        for u, line in enumerate(f, start=1):
            if u > n:
                break
            s = line.strip()
            if not s:
                continue
            toks = s.split()
            step = 2 if edge_weights else 1
            pu = assignments[u - 1]
            for i in range(0, len(toks), step):
                tok = toks[i]
                if not tok.lstrip("-").isdigit():
                    continue
                v = int(tok)
                if v <= u or v < 1 or v > n:
                    continue
                pv = assignments[v - 1]
                if pu != pv:
                    cut += 1
                    boundary[u - 1] = 1
                    boundary[v - 1] = 1
                    part_comm[pu] += 1
                    part_comm[pv] += 1
    counts = Counter(assignments)
    max_count = max(counts.values()) if counts else 0
    avg = (len(assignments) / len(counts)) if counts else 0.0
    balance = (max_count / avg) if avg else 0.0
    max_comm_vol = max(part_comm.values()) if part_comm else 0
    return {
        "graph_nodes": str(n),
        "graph_edges": str(m),
        "cut": str(cut),
        "finalobjective": str(cut),
        "bnd": str(sum(boundary)),
        "balance": f"{balance:.6f}".rstrip("0").rstrip("."),
        "max_comm_vol": str(max_comm_vol),
    }

def detect_times():
    io_time = ""
    time_partition = wall_time
    if algorithm == "kahip_fast":
        io_time = grab(r"^\s*io time:\s*([0-9.]+)\s*$")
        time_partition = grab(r"^\s*time spent for partitioning\s*([0-9.]+)\s*$") or wall_time
    elif algorithm == "metis":
        io_time = grab(r"^\s*I/O:\s*([0-9.]+)\s*sec\s*$")
        time_partition = grab(r"^\s*Partitioning:\s*([0-9.]+)\s*sec") or wall_time
    elif algorithm in {"scotch", "ptscotch", "parmetis"}:
        time_partition = grab(r"^\[SUMMARY\]\s*wall_time_sec=([0-9.]+)\s*$") or wall_time
    return io_time, time_partition

metrics = compute_metrics(graph_file, read_assignments(part_file))
io_time_sec, time_partition_sec = detect_times()

fieldnames = [
    "run_label",
    "algorithm",
    "k",
    "io_time_sec",
    "time_partition_sec",
    "graph_nodes",
    "graph_edges",
    "cut",
    "finalobjective",
    "bnd",
    "balance",
    "max_comm_vol",
    "notes",
    "prepare_cmd",
    "run_cmd",
    "output_partition_file",
    "graph_file",
    "node_index_file",
]

row = {
    "run_label": run_label,
    "algorithm": algorithm,
    "k": k,
    "io_time_sec": io_time_sec,
    "time_partition_sec": time_partition_sec,
    "graph_nodes": metrics["graph_nodes"],
    "graph_edges": metrics["graph_edges"],
    "cut": metrics["cut"],
    "finalobjective": metrics["finalobjective"],
    "bnd": metrics["bnd"],
    "balance": metrics["balance"],
    "max_comm_vol": metrics["max_comm_vol"],
    "notes": notes_override,
    "prepare_cmd": prepare_cmd,
    "run_cmd": run_cmd,
    "output_partition_file": str(part_file.resolve()),
    "graph_file": str(graph_file.resolve()),
    "node_index_file": str(node_index_file.resolve()),
}

rows = []
if summary_csv.exists():
    with summary_csv.open("r", encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f))

updated = False
for existing in rows:
    if existing.get("run_label") == run_label:
        existing.update(row)
        updated = True
        break
if not updated:
    rows.append(row)

summary_csv.parent.mkdir(parents=True, exist_ok=True)
with summary_csv.open("w", encoding="utf-8", newline="") as f:
    w = csv.DictWriter(f, fieldnames=fieldnames)
    w.writeheader()
    w.writerows(rows)
print(f"[OK] Updated summary CSV: {summary_csv}")

try:
    from openpyxl import Workbook
    wb = Workbook()
    ws = wb.active
    ws.title = "summary"
    ws.append(fieldnames)
    for existing in rows:
      ws.append([existing.get(k, "") for k in fieldnames])
    wb.save(summary_xlsx)
    print(f"[OK] Updated summary XLSX: {summary_xlsx}")
except Exception as exc:
    print(f"[WARN] Could not update summary XLSX: {exc}")
PY
}

build_rcp_comparable_assignment() {
  local full_part_file="$1"
  local graph_file="$2"
  local out_file="$3"

  python3 - "$full_part_file" "$graph_file" "$out_file" <<'PY'
import sys
from pathlib import Path

full_part = Path(sys.argv[1])
graph_file = Path(sys.argv[2])
out_file = Path(sys.argv[3])

def graph_nodes(path):
    with path.open("r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            s = line.strip()
            if not s or s.startswith("%"):
                continue
            return int(s.split()[0])
    raise RuntimeError(f"Missing METIS header: {path}")

need = graph_nodes(graph_file)
assignments = []
with full_part.open("r", encoding="utf-8", errors="ignore") as f:
    for line in f:
        s = line.strip()
        if s:
            assignments.append(s)

if len(assignments) < need:
    raise RuntimeError(f"RCP assignment shorter than graph nodes: {len(assignments)} < {need}")

out_file.parent.mkdir(parents=True, exist_ok=True)
with out_file.open("w", encoding="utf-8", newline="") as f:
    for value in assignments[:need]:
        f.write(f"{value}\n")
PY
}

update_rcp_summaries() {
  local run_label="$1"
  local k="$2"
  local log_file="$3"
  local full_part_file="$4"
  local comparable_part_file="$5"
  local graph_file="$6"
  local node_index_file="$7"
  local prepare_cmd="$8"
  local run_cmd="$9"
  local metrics_file="${10}"
  local region_nodes_file="${11}"
  local wall_time="${12}"

  python3 - "$run_label" "$k" "$log_file" "$full_part_file" "$comparable_part_file" "$graph_file" "$node_index_file" "$prepare_cmd" "$run_cmd" "$metrics_file" "$region_nodes_file" "$wall_time" "$SUMMARY_CSV" "$SUMMARY_XLSX" "$RCP_SUMMARY_CSV" "$RCP_SUMMARY_XLSX" <<'PY'
import csv
import json
import sys
from collections import Counter, defaultdict
from pathlib import Path

run_label = sys.argv[1]
k = sys.argv[2]
log_file = Path(sys.argv[3])
full_part_file = Path(sys.argv[4])
comparable_part_file = Path(sys.argv[5])
graph_file = Path(sys.argv[6])
node_index_file = Path(sys.argv[7])
prepare_cmd = sys.argv[8]
run_cmd = sys.argv[9]
metrics_file = Path(sys.argv[10])
region_nodes_file = Path(sys.argv[11])
wall_time = sys.argv[12]
summary_csv = Path(sys.argv[13])
summary_xlsx = Path(sys.argv[14])
rcp_summary_csv = Path(sys.argv[15])
rcp_summary_xlsx = Path(sys.argv[16])

def fmt_float(value):
    return f"{value:.6f}".rstrip("0").rstrip(".")

def parse_graph_header(path):
    with path.open("r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            s = line.strip()
            if not s or s.startswith("%"):
                continue
            parts = s.split()
            if len(parts) < 2:
                raise RuntimeError(f"Invalid METIS header: {path}")
            return int(parts[0]), int(parts[1]), parts[2] if len(parts) >= 3 else "0"
    raise RuntimeError(f"Missing METIS header: {path}")

def read_assignments(path):
    vals = []
    with path.open("r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            s = line.strip()
            if s:
                vals.append(int(s))
    return vals

def compute_metrics(graph_path, assignments):
    n, m, fmt = parse_graph_header(graph_path)
    if len(assignments) != n:
        raise RuntimeError(f"Assignment length mismatch: {len(assignments)} != {n}")
    edge_weights = fmt.endswith("1")
    cut = 0
    boundary = bytearray(n)
    part_comm = defaultdict(int)
    with graph_path.open("r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            s = line.strip()
            if s and not s.startswith("%"):
                break
        for u, line in enumerate(f, start=1):
            if u > n:
                break
            s = line.strip()
            if not s:
                continue
            toks = s.split()
            step = 2 if edge_weights else 1
            pu = assignments[u - 1]
            for i in range(0, len(toks), step):
                tok = toks[i]
                if not tok.lstrip("-").isdigit():
                    continue
                v = int(tok)
                if v <= u or v < 1 or v > n:
                    continue
                pv = assignments[v - 1]
                if pu != pv:
                    cut += 1
                    boundary[u - 1] = 1
                    boundary[v - 1] = 1
                    part_comm[pu] += 1
                    part_comm[pv] += 1
    counts = Counter(assignments)
    max_count = max(counts.values()) if counts else 0
    avg = (len(assignments) / len(counts)) if counts else 0.0
    balance = (max_count / avg) if avg else 0.0
    max_comm_vol = max(part_comm.values()) if part_comm else 0
    return {
        "graph_nodes": str(n),
        "graph_edges": str(m),
        "cut": str(cut),
        "finalobjective": str(cut),
        "bnd": str(sum(boundary)),
        "balance": fmt_float(balance),
        "max_comm_vol": str(max_comm_vol),
    }

def parse_region_partition_sizes(path):
    sizes = {}
    current = None
    with path.open("r", encoding="utf-8", errors="ignore") as f:
        for raw in f:
            line = raw.strip()
            if not line:
                continue
            if line.startswith("partition_id:"):
                current = int(line.split(":", 1)[1].strip())
            elif line.startswith("partition_size:") and current is not None:
                sizes[current] = int(line.split(":", 1)[1].strip())
    if not sizes:
        raise RuntimeError(f"No partition sizes found in {path}")
    return [sizes[idx] for idx in sorted(sizes)]

def update_csv(path, fieldnames, row):
    rows = []
    if path.exists():
        with path.open("r", encoding="utf-8", newline="") as f:
            rows = list(csv.DictReader(f))
    updated = False
    for existing in rows:
        if existing.get("run_label") == row["run_label"]:
            existing.update(row)
            updated = True
            break
    if not updated:
        rows.append(row)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)
    return rows

def write_xlsx(path, title, fieldnames, rows):
    try:
        from openpyxl import Workbook
        wb = Workbook()
        ws = wb.active
        ws.title = title
        ws.append(fieldnames)
        for row in rows:
            ws.append([row.get(k, "") for k in fieldnames])
        wb.save(path)
        return None
    except Exception as exc:
        return str(exc)

metrics = json.loads(metrics_file.read_text(encoding="utf-8"))
common_metrics = compute_metrics(graph_file, read_assignments(comparable_part_file))
partition_sizes = parse_region_partition_sizes(region_nodes_file)
replicated_total_nodes = sum(partition_sizes)
partition_count = len(partition_sizes)
avg_replicated = (replicated_total_nodes / partition_count) if partition_count else 0.0
balance_with_duplicates = (max(partition_sizes) / avg_replicated) if avg_replicated else 0.0
unique_assigned_nodes = replicated_total_nodes - int(metrics.get("duplicated_node_copies", 0))
replication_factor = (replicated_total_nodes / unique_assigned_nodes) if unique_assigned_nodes > 0 else 0.0

notes_parts = [
    f"duplicated_nodes={metrics.get('duplicated_nodes', 0)}",
    f"duplicated_node_copies={metrics.get('duplicated_node_copies', 0)}",
    f"component_count={metrics.get('component_count', 0)}",
    f"primary_balance={common_metrics['balance']}",
    f"balance_with_duplicates={fmt_float(balance_with_duplicates)}",
]
notes = "; ".join(notes_parts)

common_fieldnames = [
    "run_label",
    "algorithm",
    "k",
    "io_time_sec",
    "time_partition_sec",
    "graph_nodes",
    "graph_edges",
    "cut",
    "finalobjective",
    "bnd",
    "balance",
    "max_comm_vol",
    "notes",
    "prepare_cmd",
    "run_cmd",
    "output_partition_file",
    "graph_file",
    "node_index_file",
]
common_row = {
    "run_label": run_label,
    "algorithm": "rcp",
    "k": k,
    "io_time_sec": "",
    "time_partition_sec": wall_time,
    "graph_nodes": common_metrics["graph_nodes"],
    "graph_edges": common_metrics["graph_edges"],
    "cut": common_metrics["cut"],
    "finalobjective": common_metrics["finalobjective"],
    "bnd": common_metrics["bnd"],
    "balance": fmt_float(balance_with_duplicates),
    "max_comm_vol": common_metrics["max_comm_vol"],
    "notes": notes,
    "prepare_cmd": prepare_cmd,
    "run_cmd": run_cmd,
    "output_partition_file": str(comparable_part_file.resolve()),
    "graph_file": str(graph_file.resolve()),
    "node_index_file": str(node_index_file.resolve()),
}
rows = update_csv(summary_csv, common_fieldnames, common_row)
print(f"[OK] Updated summary CSV: {summary_csv}")
err = write_xlsx(summary_xlsx, "summary", common_fieldnames, rows)
if err is None:
    print(f"[OK] Updated summary XLSX: {summary_xlsx}")
else:
    print(f"[WARN] Could not update summary XLSX: {err}")

rcp_fieldnames = [
    "run_label",
    "algorithm",
    "k",
    "io_time_sec",
    "time_partition_sec",
    "graph_nodes",
    "graph_edges",
    "common_cut",
    "common_finalobjective",
    "common_bnd",
    "common_balance",
    "balance_with_duplicates",
    "common_max_comm_vol",
    "duplicated_nodes",
    "duplicated_node_copies",
    "component_count",
    "rcp_total_nodes",
    "unique_assigned_nodes",
    "replicated_total_nodes",
    "replication_factor",
    "notes",
    "prepare_cmd",
    "run_cmd",
    "output_partition_file",
    "comparable_partition_file",
    "graph_file",
    "node_index_file",
    "metrics_file",
    "region_nodes_file",
]
rcp_row = {
    "run_label": run_label,
    "algorithm": "rcp",
    "k": k,
    "io_time_sec": "",
    "time_partition_sec": wall_time,
    "graph_nodes": common_metrics["graph_nodes"],
    "graph_edges": common_metrics["graph_edges"],
    "common_cut": common_metrics["cut"],
    "common_finalobjective": common_metrics["finalobjective"],
    "common_bnd": common_metrics["bnd"],
    "common_balance": common_metrics["balance"],
    "balance_with_duplicates": fmt_float(balance_with_duplicates),
    "common_max_comm_vol": common_metrics["max_comm_vol"],
    "duplicated_nodes": str(metrics.get("duplicated_nodes", 0)),
    "duplicated_node_copies": str(metrics.get("duplicated_node_copies", 0)),
    "component_count": str(metrics.get("component_count", 0)),
    "rcp_total_nodes": str(metrics.get("total_nodes", 0)),
    "unique_assigned_nodes": str(unique_assigned_nodes),
    "replicated_total_nodes": str(replicated_total_nodes),
    "replication_factor": fmt_float(replication_factor),
    "notes": notes,
    "prepare_cmd": prepare_cmd,
    "run_cmd": run_cmd,
    "output_partition_file": str(full_part_file.resolve()),
    "comparable_partition_file": str(comparable_part_file.resolve()),
    "graph_file": str(graph_file.resolve()),
    "node_index_file": str(node_index_file.resolve()),
    "metrics_file": str(metrics_file.resolve()),
    "region_nodes_file": str(region_nodes_file.resolve()),
}
rcp_rows = update_csv(rcp_summary_csv, rcp_fieldnames, rcp_row)
print(f"[OK] Updated RCP summary CSV: {rcp_summary_csv}")
err = write_xlsx(rcp_summary_xlsx, "rcp_summary", rcp_fieldnames, rcp_rows)
if err is None:
    print(f"[OK] Updated RCP summary XLSX: {rcp_summary_xlsx}")
else:
    print(f"[WARN] Could not update RCP summary XLSX: {err}")
PY
}

should_materialize_property_graph() {
  local input_dir="${1:-$PREP_INPUT_DIR}"
  [[ "$MATERIALIZE_PROPERTY_GRAPH" -eq 1 ]] || return 1
  [[ -f "$MATERIALIZE_SCRIPT" ]] || {
    echo "[WARN] Materialization script missing: $MATERIALIZE_SCRIPT" | tee -a "$MASTER_LOG"
    return 1
  }
  [[ -d "$input_dir" ]] || {
    echo "[WARN] Materialization skipped: prep input dir not found: $input_dir" | tee -a "$MASTER_LOG"
    return 1
  }
  find "$input_dir" -maxdepth 1 -type f -name '*.csv' | read -r _ || {
    echo "[WARN] Materialization skipped: no CSV files under $input_dir" | tee -a "$MASTER_LOG"
    return 1
  }
}

materialize_partition_output() {
  local run_label="$1"
  local algorithm="$2"
  local k="$3"
  local part_file="$4"
  local node_index_file="$5"
  local out_dir="$6"
  local log_file="$7"
  local memberships_file="${8:-}"
  local input_dir_override="${9:-$PREP_INPUT_DIR}"

  if ! should_materialize_property_graph "$input_dir_override"; then
    return 0
  fi

  local materialized_root="$out_dir/materialized_property_graph"
  local cmd=(
    python3 "$MATERIALIZE_SCRIPT"
    --input-dir "$input_dir_override"
    --node-index "$node_index_file"
    --assignment "$part_file"
    --k "$k"
    --out-root "$materialized_root"
    --edge-mode "$MATERIALIZE_EDGE_MODE"
    --clean
  )
  if [[ -n "$memberships_file" ]]; then
    cmd+=(--memberships-file "$memberships_file")
  fi

  echo "[INFO] Materializing property graph for $run_label into $materialized_root" | tee -a "$MASTER_LOG" | tee -a "$log_file"
  if ! run_logged_append "$log_file" "${cmd[@]}"; then
    echo "[WARN] Materialization failed for $run_label" | tee -a "$MASTER_LOG" | tee -a "$log_file"
    return 0
  fi
  echo "[OK] Materialized property graph for $algorithm k=$k -> $materialized_root" | tee -a "$MASTER_LOG" | tee -a "$log_file"
}

run_one_algorithm() {
  local algorithm="$1"
  local k="$2"
  local out_dir="$ARTIFACTS_ROOT/${algorithm}_${DATASET_LABEL}_k${k}"
  local log_file="$RESULTS_ROOT/logs/${algorithm}_${DATASET_LABEL}_k${k}.log"
  local node_index_out="$out_dir/node_index.tsv"
  local part_file=""
  local run_label="${DATASET_LABEL}_${algorithm}_k${k}"
  local run_cmd=""
  local prepare_cmd=""
  local notes=""

  mkdir -p "$out_dir"
  prepare_cmd="prepared input: $GRAPH_FILE"

  case "$algorithm" in
    kahip_fast)
      local kahip_bin
      kahip_bin="$(choose_binary_optional "$KAHIP_BIN" \
        "$PROJECT_DIR/algorithms/KaHIP/deploy/kaffpa" \
        "$WORKSPACE_DIR/graph_partition_gui_portable/algorithms/KaHIP/deploy/kaffpa" \
        "$WORKSPACE_DIR/KaHIP/deploy/kaffpa")" || {
        echo "[ERROR] Could not find kaffpa. Use --kahip-bin." >&2
        return 1
      }
      part_file="$out_dir/kahip_fast_partition_k${k}.txt"
      run_cmd="$kahip_bin $GRAPH_FILE --k $k --preconfiguration=fast --seed $SEED --output_filename $part_file"
      local start_ns end_ns wall_time
      start_ns="$(date +%s%N)"
      run_logged "$log_file" "$kahip_bin" "$GRAPH_FILE" --k "$k" --preconfiguration=fast --seed "$SEED" --output_filename "$part_file"
      end_ns="$(date +%s%N)"
      wall_time="$(python3 - "$start_ns" "$end_ns" <<'PY'
import sys
s = int(sys.argv[1]); e = int(sys.argv[2])
print(f"{(e - s) / 1_000_000_000.0:.6f}".rstrip("0").rstrip("."))
PY
)"
      copy_node_index "$NODE_INDEX_FILE" "$node_index_out"
      update_summary "$run_label" "$algorithm" "$k" "$log_file" "$part_file" "$GRAPH_FILE" "$node_index_out" "$prepare_cmd" "$run_cmd" "$notes" "$wall_time"
      materialize_partition_output "$run_label" "$algorithm" "$k" "$part_file" "$node_index_out" "$out_dir" "$log_file"
      ;;
    metis)
      local metis_bin
      metis_bin="$(choose_binary_optional "$METIS_BIN" \
        "$WORKSPACE_DIR/graph_partition_gui_portable/algorithms/METIS/build/programs/gpmetis" \
        "$WORKSPACE_DIR/METIS/build/programs/gpmetis")" || {
        echo "[ERROR] Could not find gpmetis. Use --metis-bin." >&2
        return 1
      }
      local metis_lib
      metis_lib="$(dirname "$metis_bin")"
      local metis_parent
      metis_parent="$(dirname "$metis_lib")"
      local ld_paths=()
      if [[ -d "$metis_parent/lib" ]]; then
        ld_paths+=("$metis_parent/lib")
      fi
      if [[ -d "$metis_parent/libmetis" ]]; then
        ld_paths+=("$metis_parent/libmetis")
      fi
      if [[ -d "$(dirname "$metis_parent")/GKlib/lib" ]]; then
        ld_paths+=("$(dirname "$metis_parent")/GKlib/lib")
      fi
      part_file="$out_dir/metis_partition_k${k}.txt"
      run_cmd="$metis_bin $GRAPH_FILE $k"
      local start_ns end_ns wall_time tmp_part
      tmp_part="${GRAPH_FILE}.part.${k}"
      rm -f "$tmp_part"
      start_ns="$(date +%s%N)"
      if [[ "${#ld_paths[@]}" -gt 0 ]]; then
        local joined
        joined="$(IFS=:; echo "${ld_paths[*]}")"
        run_logged_shell "$log_file" "export LD_LIBRARY_PATH='$joined':\${LD_LIBRARY_PATH:-}; '$metis_bin' '$GRAPH_FILE' '$k'"
      else
        run_logged "$log_file" "$metis_bin" "$GRAPH_FILE" "$k"
      fi
      end_ns="$(date +%s%N)"
      wall_time="$(python3 - "$start_ns" "$end_ns" <<'PY'
import sys
s = int(sys.argv[1]); e = int(sys.argv[2])
print(f"{(e - s) / 1_000_000_000.0:.6f}".rstrip("0").rstrip("."))
PY
)"
      if [[ ! -f "$tmp_part" ]]; then
        echo "[ERROR] METIS output missing: $tmp_part" >&2
        return 1
      fi
      cp -f "$tmp_part" "$part_file"
      copy_node_index "$NODE_INDEX_FILE" "$node_index_out"
      update_summary "$run_label" "$algorithm" "$k" "$log_file" "$part_file" "$GRAPH_FILE" "$node_index_out" "$prepare_cmd" "$run_cmd" "$notes" "$wall_time"
      materialize_partition_output "$run_label" "$algorithm" "$k" "$part_file" "$node_index_out" "$out_dir" "$log_file"
      ;;
    parmetis)
      local parmetis_bin
      parmetis_bin="$(choose_binary_optional "$PARMETIS_BIN" \
        "$PROJECT_DIR/algorithms/parmetis/bin/parmetis" \
        "$PROJECT_DIR/algorithms/parmetis/bin/pm_parmetis" \
        "$PROJECT_DIR/algorithms/ParMETIS/build/Linux-x86_64/programs/parmetis" \
        "$PROJECT_DIR/algorithms/ParMETIS/build/Linux-x86_64/programs/pm_parmetis")" || {
        echo "[ERROR] Could not find parmetis. Use --parmetis-bin." >&2
        return 1
      }
      part_file="$out_dir/parmetis_partition_k${k}.txt"
      run_cmd="mpirun --oversubscribe -np $MPI_PROCS $parmetis_bin $GRAPH_FILE 1 $k 1000 1000.0 0 $SEED"
      local start_ns end_ns wall_time
      start_ns="$(date +%s%N)"
      run_logged "$log_file" run_parmetis_default "$GRAPH_FILE" "$k" "$part_file" "$MPI_PROCS"
      end_ns="$(date +%s%N)"
      wall_time="$(python3 - "$start_ns" "$end_ns" <<'PY'
import sys
s = int(sys.argv[1]); e = int(sys.argv[2])
print(f"{(e - s) / 1_000_000_000.0:.6f}".rstrip("0").rstrip("."))
PY
)"
      if [[ ! -f "$part_file" ]]; then
        echo "[ERROR] ParMETIS output missing after run: $part_file" >&2
        return 1
      fi
      echo "[SUMMARY] wall_time_sec=$wall_time" | tee -a "$log_file"
      copy_node_index "$NODE_INDEX_FILE" "$node_index_out"
      update_summary "$run_label" "$algorithm" "$k" "$log_file" "$part_file" "$GRAPH_FILE" "$node_index_out" "$prepare_cmd" "$run_cmd" "$notes" "$wall_time"
      materialize_partition_output "$run_label" "$algorithm" "$k" "$part_file" "$node_index_out" "$out_dir" "$log_file"
      ;;
    scotch)
      part_file="$out_dir/scotch_partition_k${k}.txt"
      if [[ -n "$SCOTCH_CMD" ]]; then
        run_cmd="$(fill_template "$SCOTCH_CMD" "$GRAPH_FILE" "$k" "$part_file" 1)"
      else
        run_cmd="internal scotch wrapper graph=$GRAPH_FILE k=$k out=$part_file"
      fi
      local start_ns end_ns wall_time
      start_ns="$(date +%s%N)"
      if [[ -n "$SCOTCH_CMD" ]]; then
        run_logged_shell "$log_file" "$run_cmd"
      else
        run_logged "$log_file" run_scotch_default "$GRAPH_FILE" "$k" "$part_file"
      fi
      end_ns="$(date +%s%N)"
      wall_time="$(python3 - "$start_ns" "$end_ns" <<'PY'
import sys
s = int(sys.argv[1]); e = int(sys.argv[2])
print(f"{(e - s) / 1_000_000_000.0:.6f}".rstrip("0").rstrip("."))
PY
)"
      echo "[SUMMARY] wall_time_sec=$wall_time" | tee -a "$log_file"
      copy_node_index "$NODE_INDEX_FILE" "$node_index_out"
      update_summary "$run_label" "$algorithm" "$k" "$log_file" "$part_file" "$GRAPH_FILE" "$node_index_out" "$prepare_cmd" "$run_cmd" "$notes" "$wall_time"
      materialize_partition_output "$run_label" "$algorithm" "$k" "$part_file" "$node_index_out" "$out_dir" "$log_file"
      ;;
    ptscotch)
      part_file="$out_dir/scotch_partition_k${k}.txt"
      if [[ -n "$PTSCOTCH_CMD" ]]; then
        run_cmd="$(fill_template "$PTSCOTCH_CMD" "$GRAPH_FILE" "$k" "$part_file" "$MPI_PROCS")"
      else
        run_cmd="internal ptscotch wrapper graph=$GRAPH_FILE k=$k out=$part_file mpi=$MPI_PROCS"
      fi
      local start_ns end_ns wall_time
      start_ns="$(date +%s%N)"
      if [[ -n "$PTSCOTCH_CMD" ]]; then
        run_logged_shell "$log_file" "$run_cmd"
      else
        run_logged "$log_file" run_ptscotch_default "$GRAPH_FILE" "$k" "$part_file" "$MPI_PROCS"
      fi
      end_ns="$(date +%s%N)"
      wall_time="$(python3 - "$start_ns" "$end_ns" <<'PY'
import sys
s = int(sys.argv[1]); e = int(sys.argv[2])
print(f"{(e - s) / 1_000_000_000.0:.6f}".rstrip("0").rstrip("."))
PY
)"
      echo "[SUMMARY] wall_time_sec=$wall_time" | tee -a "$log_file"
      copy_node_index "$NODE_INDEX_FILE" "$node_index_out"
      update_summary "$run_label" "$algorithm" "$k" "$log_file" "$part_file" "$GRAPH_FILE" "$node_index_out" "$prepare_cmd" "$run_cmd" "$notes" "$wall_time"
      materialize_partition_output "$run_label" "$algorithm" "$k" "$part_file" "$node_index_out" "$out_dir" "$log_file"
      ;;
    rcp)
      if [[ ! -x "$RCP_RUNNER" ]]; then
        echo "[ERROR] RCP runner not found or not executable: $RCP_RUNNER" >&2
        return 1
      fi
      local rcp_input_dir="$PREP_INPUT_DIR"
      if [[ "$DATASET_LABEL" == "cordis_horizon_inputs" ]]; then
        local cordis_rcp_dir="$DATASET_DIR/rcp_neo4j_csv"
        if [[ ! -d "$cordis_rcp_dir" || ! -f "$cordis_rcp_dir/Project.csv" ]]; then
          echo "[INFO] Preparing CORDIS Neo4j-style CSV projection for RCP under $cordis_rcp_dir" | tee -a "$MASTER_LOG" | tee -a "$log_file"
          run_logged "$log_file" python3 "$PREP_CORDIS_RCP_SCRIPT" --dataset-dir "$DATASET_DIR" --out-dir "$cordis_rcp_dir"
        fi
        rcp_input_dir="$cordis_rcp_dir"
      fi
      if [[ ! -d "$rcp_input_dir" ]]; then
        echo "[ERROR] RCP requires a Neo4j CSV input dir: $rcp_input_dir" >&2
        return 1
      fi
      part_file="$out_dir/rcp_partition_k${k}.txt"
      local metrics_file="$out_dir/rcp_metrics.json"
      local comparable_part_file="$out_dir/rcp_partition_k${k}_comparable.txt"
      local region_nodes_file="$out_dir/work/region_node_component_${k}_v2.txt"
      run_cmd="bash $RCP_RUNNER --input-dir $rcp_input_dir --node-index $NODE_INDEX_FILE --k $k --out-dir $out_dir"
      local start_ns end_ns wall_time
      start_ns="$(date +%s%N)"
      run_logged "$log_file" bash "$RCP_RUNNER" --input-dir "$rcp_input_dir" --node-index "$NODE_INDEX_FILE" --k "$k" --out-dir "$out_dir"
      end_ns="$(date +%s%N)"
      wall_time="$(python3 - "$start_ns" "$end_ns" <<'PY'
import sys
s = int(sys.argv[1]); e = int(sys.argv[2])
print(f"{(e - s) / 1_000_000_000.0:.6f}".rstrip("0").rstrip("."))
PY
)"
      if [[ ! -f "$part_file" ]]; then
        echo "[ERROR] RCP output missing: $part_file" >&2
        return 1
      fi
      if [[ ! -f "$metrics_file" ]]; then
        echo "[ERROR] RCP metrics output missing: $metrics_file" >&2
        return 1
      fi
      if [[ ! -f "$region_nodes_file" ]]; then
        echo "[ERROR] RCP region-node output missing: $region_nodes_file" >&2
        return 1
      fi
      build_rcp_comparable_assignment "$part_file" "$GRAPH_FILE" "$comparable_part_file"
      copy_node_index "$NODE_INDEX_FILE" "$node_index_out"
      update_rcp_summaries "$run_label" "$k" "$log_file" "$part_file" "$comparable_part_file" "$GRAPH_FILE" "$node_index_out" "$prepare_cmd" "$run_cmd" "$metrics_file" "$region_nodes_file" "$wall_time"
      materialize_partition_output "$run_label" "$algorithm" "$k" "$part_file" "$node_index_out" "$out_dir" "$log_file" "$region_nodes_file" "$rcp_input_dir"
      ;;
    *)
      echo "[ERROR] Unsupported algorithm: $algorithm" >&2
      return 1
      ;;
  esac
}

run_with_retries() {
  local name="$1"
  shift
  local attempt=0
  local max_attempts=$((RETRIES + 1))

  while (( attempt < max_attempts )); do
    attempt=$((attempt + 1))
    echo "[INFO] Running $name (attempt $attempt/$max_attempts)" | tee -a "$MASTER_LOG"
    if "$@"; then
      echo "[OK] $name finished" | tee -a "$MASTER_LOG"
      return 0
    fi
    echo "[WARN] $name failed on attempt $attempt" | tee -a "$MASTER_LOG"
  done

  echo "[ERROR] $name failed after $max_attempts attempt(s)." | tee -a "$MASTER_LOG"
  return 1
}

prepare_dataset() {
  case "$PREP_MODE" in
    prepared)
      echo "[INFO] Using existing prepared inputs under $PREP_DIR" | tee -a "$MASTER_LOG"
      ;;
    neo4j_csv)
      if [[ ! -d "$PREP_INPUT_DIR" ]]; then
        echo "[ERROR] Prep input dir not found: $PREP_INPUT_DIR" | tee -a "$MASTER_LOG"
        return 1
      fi
      run_with_retries "${DATASET_LABEL}_prepare" python3 "$PREP_NEO4J_SCRIPT" --input-dir "$PREP_INPUT_DIR" --out-dir "$PREP_DIR"
      ;;
    dblp_xml)
      if [[ -z "$RAW_XML" || ! -f "$RAW_XML" ]]; then
        echo "[ERROR] DBLP XML not found: $RAW_XML" | tee -a "$MASTER_LOG"
        return 1
      fi
      local -a prep_cmd=(python3 "$PREP_DBLP_SCRIPT" --raw-xml "$RAW_XML" --dataset-dir "$DATASET_DIR")
      if [[ "$MAX_RECORDS" -gt 0 ]]; then
        prep_cmd+=(--max-records "$MAX_RECORDS")
      fi
      run_with_retries "${DATASET_LABEL}_prepare" "${prep_cmd[@]}"
      ;;
    cordis_horizon)
      if [[ -z "$RAW_ZIP_DIR" || ! -d "$RAW_ZIP_DIR" ]]; then
        echo "[ERROR] CORDIS zip dir not found: $RAW_ZIP_DIR" | tee -a "$MASTER_LOG"
        return 1
      fi
      run_with_retries "${DATASET_LABEL}_prepare" python3 "$PREP_CORDIS_SCRIPT" --raw-zip-dir "$RAW_ZIP_DIR" --dataset-dir "$DATASET_DIR"
      ;;
    *)
      echo "[ERROR] Unknown prep mode: $PREP_MODE" | tee -a "$MASTER_LOG"
      return 1
      ;;
  esac
}

if ! prepare_dataset; then
  echo "[ERROR] Preparation failed; cannot continue." | tee -a "$MASTER_LOG"
  exit 1
fi

if [[ ! -f "$GRAPH_FILE" || ! -f "$NODE_INDEX_FILE" || ! -f "$HASH_NODES_FILE" || ! -d "$HASH_REL_DIR" ]]; then
  echo "[ERROR] Missing prepared inputs under $PREP_DIR" | tee -a "$MASTER_LOG"
  exit 1
fi

IFS=',' read -r -a KS_ARRAY <<< "$KS_CSV"
failed=()

for raw_k in "${KS_ARRAY[@]}"; do
  k="$(echo "$raw_k" | xargs)"
  if [[ ! "$k" =~ ^[0-9]+$ ]] || [[ "$k" -lt 2 ]]; then
    echo "[ERROR] Invalid k value: $k" | tee -a "$MASTER_LOG"
    exit 1
  fi

  for algorithm in "${SELECTED_ALGORITHMS[@]}"; do
    if ! run_with_retries "${DATASET_LABEL}_${algorithm}_k${k}" run_one_algorithm "$algorithm" "$k"; then
      failed+=("${algorithm}_k${k}")
      if [[ "$CONTINUE_ON_ERROR" -eq 0 ]]; then
        echo "[ERROR] master-log=$MASTER_LOG" | tee -a "$MASTER_LOG"
        exit 1
      fi
    fi
  done
done

if [[ "${#failed[@]}" -gt 0 ]]; then
  echo "[ERROR] Finished with failures: ${failed[*]}" | tee -a "$MASTER_LOG"
  echo "[ERROR] master-log=$MASTER_LOG" | tee -a "$MASTER_LOG"
  exit 1
fi

echo "[OK] All algorithms completed successfully for $DATASET_LABEL." | tee -a "$MASTER_LOG"
echo "[OK] master-log=$MASTER_LOG" | tee -a "$MASTER_LOG"
