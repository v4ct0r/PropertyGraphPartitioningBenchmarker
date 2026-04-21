# Query Sets For Dataset Testing

These query folders are ready for `neo4j/scripts/run_query_bench.py`:

- `neo4j/queries_ldbc_test` (9 queries)
- `neo4j/queries_fib25_test` (10 queries)
- `neo4j/queries_mb6_test` (10 queries)
- `neo4j/queries_cordis_test` (8 queries)

## Run examples

LDBC:

```bash
python3 neo4j/scripts/run_query_bench.py \
  --uri bolt://localhost:7687 \
  --user neo4j --password test12345 \
  --case ldbc_test \
  --query-dir neo4j/queries_ldbc_test \
  --out neo4j/results/query_latency_from_spark.csv
```

fib25:

```bash
python3 neo4j/scripts/run_query_bench.py \
  --uri bolt://localhost:7687 \
  --user neo4j --password test12345 \
  --case fib25_test \
  --query-dir neo4j/queries_fib25_test \
  --out neo4j/results/query_latency_from_spark.csv
```

mb6:

```bash
python3 neo4j/scripts/run_query_bench.py \
  --uri bolt://localhost:7687 \
  --user neo4j --password test12345 \
  --case mb6_test \
  --query-dir neo4j/queries_mb6_test \
  --out neo4j/results/query_latency_from_spark.csv
```

CORDIS:

```bash
python3 neo4j/scripts/run_query_bench.py \
  --uri bolt://localhost:7687 \
  --user neo4j --password test12345 \
  --case cordis_test \
  --query-dir neo4j/queries_cordis_test \
  --out neo4j/results/query_latency_from_spark.csv
```

## Notes

- LDBC queries are based on your existing short queries plus additional web-derived IC6/IC7/IC9-style reads.
- fib25/mb6 include schema-safe checks and additional weighted-path queries inspired by neuPrint Cypher examples.
- CORDIS queries target the reduced project-publication-deliverable graph used by the partitioning pipeline.
- If relationship type names differ in your imported graph, run `q05_neuron_rel_types.txt` first and adjust as needed.

## Web Sources Used

- LDBC individual query specs: https://ldbcouncil.org/benchmarks/snb-interactive/
- Example Cypher implementations for IC4/IC6/IC7/IC9 (Neo4j Fabric): https://gist.github.com/mneedham/e06a1aba8f8e7b3b0c5d88b630814d22
- neuPrint Cypher examples/discussion (`ConnectsTo`, weighted paths):
  https://groups.google.com/g/neuprint
- neuprintr custom Cypher interface docs (query execution pattern):
  https://natverse.org/neuprintr/reference/neuprint_custom_query.html
