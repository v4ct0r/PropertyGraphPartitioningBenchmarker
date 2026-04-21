# PropertyGraphPartitioningBenchmarker

Official native-Docker benchmark repository for property-graph partitioning.

Included:
- native Docker pipeline scripts
- required algorithm sources under `algorithms/` (RCP, METIS, ParMETIS, KaHIP, Scotch/PT-Scotch)
- prepared/property-graph dataset artifacts used by the final benchmark
- final aggregate benchmark results
- aggregate charts and Excel chart workbooks

Main entrypoints:
- `scripts/build_native_algorithms_docker.sh`
- `scripts/run_native_algorithms_in_docker.sh`
- `scripts/run_neo4j_queries_results.py`
- `scripts/materialize_partitioned_property_graph.py`
- `scripts/generate_aggregate_charts.py`

Main final outputs:
- `results/docker_comparison_summary.csv`
- `results/queries_results.csv`
- `results/materialization_metrics.csv`
- `results/charts/`

Notes:
- Some dataset files are larger than standard GitHub file limits; if this is pushed to GitHub, Git LFS or release archives may still be required.
