#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

IMAGE_TAG="property-graph-native-partitioners:latest"
DATASET=""
DATASET_DIR=""
DATASET_LABEL=""
PREP_MODE="prepared"
ALGORITHMS_CSV="kahip_fast,metis,scotch"
KS_CSV="2,4,6,8"
RESULTS_ROOT="/workspace/pgbench/results"
CPUSET_CPUS=""
AGGREGATE_SUMMARY_HOST="$PROJECT_DIR/results/docker_comparison_summary.csv"
SKIP_AGGREGATE_MERGE=0
EXTRA_ARGS=()

usage() {
  cat <<'EOF'
Run selected native graph partitioners inside the pinned Docker image.

Usage:
  bash scripts/run_native_algorithms_in_docker.sh [options]

Core options:
  --dataset <name-or-path>   Dataset name under datasets or explicit path
  --dataset-dir <path>       Explicit dataset directory
  --dataset-label <txt>      Optional label override
  --prep-mode <mode>         Default: prepared
  --algorithms <csv>         Default: kahip_fast,metis,scotch
  --ks <csv>                 Default: 2,4,6,8
  --image-tag <tag>          Docker image tag (default: property-graph-native-partitioners:latest)
  --results-root <path>      In-container results root (default: /workspace/pgbench/results)
  --cpuset-cpus <csv>        Optional Docker CPU pinning, e.g. 1-5
  --aggregate-summary <path> Host-side aggregate Docker summary CSV (default: results/docker_comparison_summary.csv)
  --no-aggregate-merge       Skip updating the host-side aggregate Docker summary CSV
  --help                     Show this help

Any unknown trailing arguments are forwarded to scripts/run_all_algorithms.sh inside the container.
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
    --ks)
      KS_CSV="${2:?missing value for --ks}"
      shift 2
      ;;
    --image-tag)
      IMAGE_TAG="${2:?missing value for --image-tag}"
      shift 2
      ;;
    --results-root)
      RESULTS_ROOT="${2:?missing value for --results-root}"
      shift 2
      ;;
    --cpuset-cpus)
      CPUSET_CPUS="${2:?missing value for --cpuset-cpus}"
      shift 2
      ;;
    --aggregate-summary)
      AGGREGATE_SUMMARY_HOST="${2:?missing value for --aggregate-summary}"
      shift 2
      ;;
    --no-aggregate-merge)
      SKIP_AGGREGATE_MERGE=1
      shift
      ;;
    --help)
      usage
      exit 0
      ;;
    *)
      EXTRA_ARGS+=("$1")
      shift
      ;;
  esac
done

if [[ -z "$DATASET" && -z "$DATASET_DIR" ]]; then
  echo "[ERROR] Provide --dataset or --dataset-dir"
  exit 1
fi

docker_args=(
  run --rm
  -u "$(id -u):$(id -g)"
  -e HOME=/tmp/codex-home
  -v "$PROJECT_DIR:/workspace/pgbench"
  -w /workspace/pgbench
)

if [[ -n "$CPUSET_CPUS" ]]; then
  docker_args+=(--cpuset-cpus "$CPUSET_CPUS")
fi

container_cmd=(
  bash scripts/run_all_algorithms.sh
  --prep-mode "$PREP_MODE"
  --algorithms "$ALGORITHMS_CSV"
  --ks "$KS_CSV"
  --results-root "$RESULTS_ROOT"
  --strict-native
  --kahip-bin /opt/algorithms/kahip/bin/kaffpa
  --metis-bin /opt/algorithms/metis/bin/gpmetis
  --parmetis-bin /opt/algorithms/parmetis/bin/parmetis
  --mpirun-bin /usr/bin/mpirun
)

if [[ -n "$DATASET" ]]; then
  container_cmd+=(--dataset "$DATASET")
fi
if [[ -n "$DATASET_DIR" ]]; then
  container_cmd+=(--dataset-dir "$DATASET_DIR")
fi
if [[ -n "$DATASET_LABEL" ]]; then
  container_cmd+=(--dataset-label "$DATASET_LABEL")
fi

container_cmd+=(
  --scotchmetis-bin /opt/algorithms/scotch/bin/gpart
)

if [[ -x "$PROJECT_DIR/algorithms/scotch-7.0.4/src/scotch/dgpart" ]]; then
  container_cmd+=(
    --ptscotchmetis-bin /opt/algorithms/scotch/bin/dgpart
  )
fi

if [[ "${#EXTRA_ARGS[@]}" -gt 0 ]]; then
  container_cmd+=("${EXTRA_ARGS[@]}")
fi

echo "[INFO] Image: $IMAGE_TAG"
echo "[INFO] Algorithms: $ALGORITHMS_CSV"
docker "${docker_args[@]}" "$IMAGE_TAG" "${container_cmd[@]}"

if [[ "$SKIP_AGGREGATE_MERGE" -eq 0 ]]; then
  python3 "$PROJECT_DIR/scripts/merge_comparison_summaries.py" \
    --output "$AGGREGATE_SUMMARY_HOST" \
    --glob "results/docker*/comparison_summary.csv"
else
  echo "[INFO] Skipped aggregate Docker summary merge"
fi
