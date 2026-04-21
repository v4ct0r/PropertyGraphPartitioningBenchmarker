#!/usr/bin/env bash
set -euo pipefail

# Minimal end-to-end helper for a fresh clone.
#
# Default behavior:
# 1. build the Docker image
# 2. run the native partitioning pipeline on one prepared dataset
# 3. optionally materialize in the same run
# 4. optionally run query benchmarking
# 5. optionally regenerate aggregate charts
#
# Prepared datasets currently present in this repo:
# - fib25_neo4j_inputs
# - mb6_neo4j_inputs
# - cordis_horizon_inputs
#
# Larger datasets may be provided separately as release archives:
# - ldbc_inputs_1_4
# - cord19_reduced_neo4j_inputs
# - cord19_full_structural_inputs
# - cord19_full_typed_light_inputs
#
# Supported native algorithms in this pipeline:
# - kahip_fast
# - metis
# - parmetis
# - scotch
# - ptscotch
# - rcp
#
# Query benchmarking is currently wired for:
# - fib25_neo4j_inputs
# - mb6_neo4j_inputs
# - ldbc_inputs_1_4
# - cordis_horizon_inputs
#
# Edit the variables below before running if you want a different dataset or k set.

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

DATASET="mb6_neo4j_inputs"
ALGORITHMS="kahip_fast,metis,parmetis,scotch,ptscotch,rcp"
KS="2"
PREP_MODE="prepared"
RESULTS_ROOT="results/test_run"
CPUSET_CPUS=""

BUILD_IMAGE=1
MATERIALIZE_INLINE=0
RUN_QUERY=0
GENERATE_CHARTS=0

# Query benchmarking needs a running Neo4j instance and only applies to datasets
# with a query workload under neo4j/queries_*.
QUERY_RESULTS_CSV="results/queries_results_test_run.csv"
QUERY_RESULTS_MD="results/queries_results_test_run.md"
QUERY_RESULTS_TXT="results/queries_results_test_run.txt"

mkdir -p results/logs

if [[ "$BUILD_IMAGE" == "1" ]]; then
  bash scripts/build/build_native_algorithms_docker.sh
fi

RUN_CMD=(
  bash scripts/run/run_native_algorithms_in_docker.sh
  --dataset "$DATASET"
  --prep-mode "$PREP_MODE"
  --algorithms "$ALGORITHMS"
  --ks "$KS"
  --results-root "/workspace/pgbench/$RESULTS_ROOT"
)

if [[ -n "$CPUSET_CPUS" ]]; then
  RUN_CMD+=(--cpuset-cpus "$CPUSET_CPUS")
fi

if [[ "$MATERIALIZE_INLINE" == "1" ]]; then
  RUN_CMD+=(--materialize-property-graph)
fi

printf 'Running partition pipeline:\n  %q' "${RUN_CMD[0]}"
for arg in "${RUN_CMD[@]:1}"; do
  printf ' %q' "$arg"
done
printf '\n'
"${RUN_CMD[@]}"

if [[ "$RUN_QUERY" == "1" ]]; then
  python3 scripts/query/run_neo4j_queries_results.py \
    --summary-csv results/docker_comparison_summary.csv \
    --datasets "$DATASET" \
    --algorithms "$ALGORITHMS" \
    --ks "$KS" \
    --results-csv "$QUERY_RESULTS_CSV" \
    --results-md "$QUERY_RESULTS_MD" \
    --results-txt "$QUERY_RESULTS_TXT"
fi

if [[ "$GENERATE_CHARTS" == "1" ]]; then
  python3 scripts/results/generate_aggregate_charts.py
fi

echo
echo "Done. Check:" 
echo "- $RESULTS_ROOT"
echo "- results/docker_comparison_summary.csv"
if [[ "$RUN_QUERY" == "1" ]]; then
  echo "- $QUERY_RESULTS_CSV"
fi
if [[ "$GENERATE_CHARTS" == "1" ]]; then
  echo "- results/charts/"
fi
