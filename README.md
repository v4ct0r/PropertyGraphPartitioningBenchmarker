# PropertyGraphPartitioningBenchmarker

Native-Docker benchmark repository for property-graph partitioning.

## How to run

1. Build the native Docker image:
- `bash scripts/build/build_native_algorithms_docker.sh`

2. Run partitioning inside Docker on a prepared dataset:
- `bash scripts/run/run_native_algorithms_in_docker.sh --dataset mb6_neo4j_inputs --prep-mode prepared --algorithms kahip_fast,metis,parmetis,scotch,ptscotch,rcp --ks 2,4,6,8`

3. Run query benchmarking:
- `python3 scripts/query/run_neo4j_queries_results.py --summary-csv results/docker_comparison_summary.csv --datasets mb6_neo4j_inputs --algorithms metis,kahip_fast,parmetis,ptscotch,scotch,rcp --ks 2,4,6,8 --results-csv results/queries_results.csv --results-md results/queries_results.md --results-txt results/queries_results.txt`

4. Generate aggregate charts:
- `python3 scripts/results/generate_aggregate_charts.py`

5. Inspect final outputs:
- `results/docker_comparison_summary.csv`
- `results/queries_results.csv`
- `results/materialization_metrics.csv`
- `results/charts/`

Included:
- native Docker pipeline scripts
- required algorithm sources under `algorithms/` (RCP, METIS, ParMETIS, KaHIP, Scotch/PT-Scotch)
- prepared/property-graph dataset artifacts used by the final benchmark
- final aggregate benchmark results
- aggregate charts and Excel chart workbooks

Main entrypoints:
- `scripts/build/build_native_algorithms_docker.sh`
- `scripts/run/run_native_algorithms_in_docker.sh`
- `scripts/query/run_neo4j_queries_results.py`
- `scripts/materialize/materialize_partitioned_property_graph.py`
- `scripts/results/generate_aggregate_charts.py`

Main final outputs:
- `results/docker_comparison_summary.csv`
- `results/queries_results.csv`
- `results/materialization_metrics.csv`
- `results/charts/`

Notes:
- Some dataset files are larger than standard GitHub file limits; if this is pushed to GitHub, Git LFS or release archives may still be required.
