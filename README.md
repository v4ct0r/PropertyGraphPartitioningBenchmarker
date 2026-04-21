# PropertyGraphPartitioningBenchmarker

Native-Docker benchmark repository for property-graph partitioning.

## How to run

1. Build the native Docker image:
- `bash scripts/build/build_native_algorithms_docker.sh`

2. Run partitioning inside Docker on a prepared dataset:
- `bash scripts/run/run_native_algorithms_in_docker.sh --dataset mb6_neo4j_inputs --prep-mode prepared --algorithms kahip_fast,metis,parmetis,scotch,ptscotch,rcp --ks 2,4,6,8`
- To materialize in the same run, add `--materialize-property-graph`.

3. Materialize partitioned CSV outputs after a completed partitioning run:
- `python3 scripts/materialize/materialize_partitioned_property_graph.py --input-dir datasets/mb6_neo4j_inputs --node-index results/<run_root>/algorithm_outputs/metis_mb6_neo4j_inputs_k2/node_index.tsv --assignment results/<run_root>/algorithm_outputs/metis_mb6_neo4j_inputs_k2/metis_partition_k2.txt --k 2 --out-root results/<run_root>/algorithm_outputs/metis_mb6_neo4j_inputs_k2/materialized_property_graph --clean`
- For RCP, pass the dataset-specific Neo4j CSV directory as `--input-dir` and add `--memberships-file results/<run_root>/algorithm_outputs/rcp_<dataset>_k2/work/region_node_component_2_v2.txt`.

4. Run query benchmarking:
- `python3 scripts/query/run_neo4j_queries_results.py --summary-csv results/docker_comparison_summary.csv --datasets mb6_neo4j_inputs --algorithms metis,kahip_fast,parmetis,ptscotch,scotch,rcp --ks 2,4,6,8 --results-csv results/queries_results.csv --results-md results/queries_results.md --results-txt results/queries_results.txt`

5. Generate aggregate charts:
- `python3 scripts/results/generate_aggregate_charts.py`

6. Inspect final outputs:
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
