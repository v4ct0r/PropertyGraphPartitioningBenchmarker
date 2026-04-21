#!/usr/bin/env python3
import argparse
import csv
import glob
import os
import re
import statistics
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple


def import_graph_database():
    try:
        from neo4j import GraphDatabase as _GraphDatabase

        return _GraphDatabase
    except Exception:
        # Running from the repo root can shadow the pip package with the local
        # `neo4j/` data directory. Remove repo paths and retry the import.
        script_dir = Path(__file__).resolve().parent
        project_dir = script_dir.parent
        cleaned = []
        for entry in sys.path:
            try:
                resolved = Path(entry or ".").resolve()
            except Exception:
                cleaned.append(entry)
                continue
            if resolved in {project_dir, script_dir}:
                continue
            cleaned.append(entry)
        sys.path[:] = cleaned
        from neo4j import GraphDatabase as _GraphDatabase

        return _GraphDatabase


GraphDatabase = import_graph_database()

RUN_LABEL_RE = re.compile(
    r"^(?P<dataset>.+)_(?P<algorithm>hash|metis|kahip_fast|scotch|ptscotch|parmetis|rcp)_k(?P<k>\d+)$"
)
INT_RE = re.compile(r"^-?\d+$")
FLOAT_RE = re.compile(r"^-?\d+\.\d+$")

PARTITION_FILE_TEMPLATE = {
    "hash": "hash_partition_k{k}.txt",
    "metis": "metis_partition_k{k}.txt",
    "kahip_fast": "kahip_fast_partition_k{k}.txt",
    "scotch": "scotch_partition_k{k}.txt",
    "ptscotch": "scotch_partition_k{k}.txt",
    "parmetis": "parmetis_partition_k{k}.txt",
    "rcp": "rcp_partition_k{k}_comparable.txt",
}

DATASET_CONFIG = {
    "ldbc_inputs_1_4": {
        "input_dir": "datasets/ldbc_inputs_1_4/combined",
        "delimiter": "|",
        "query_dir": "neo4j/queries_ldbc_test",
        "include_globs": ["*.csv"],
        "max_node_rows": 300000,
        "max_rel_rows": 600000,
        "per_file_node_cap": 3000,
        "per_file_rel_cap": 10000,
    },
    "fib25_neo4j_inputs": {
        "input_dir": "datasets/fib25_neo4j_inputs",
        "delimiter": ",",
        "query_dir": "neo4j/queries_fib25_test",
        # Keep load bounded to the entities/edges used by the query set.
        "include_globs": ["Neuprint_Neurons_*.csv", "Neuprint_Neuron_Connections_*.csv"],
        "max_node_rows": 50000,
        "max_rel_rows": 200000,
    },
    "mb6_neo4j_inputs": {
        "input_dir": "datasets/mb6_neo4j_inputs",
        "delimiter": ",",
        "query_dir": "neo4j/queries_mb6_test",
        # Keep load bounded to the entities/edges used by the query set.
        "include_globs": ["Neuprint_Neurons_*.csv", "Neuprint_Neuron_Connections_*.csv"],
        "max_node_rows": 50000,
        "max_rel_rows": 200000,
    },
    "cordis_horizon_inputs": {
        # Use the typed RCP projection rather than the raw flat CSVs so query
        # evaluation runs on the same graph topology as the partitioners.
        "input_dir": "datasets/cordis_horizon_inputs/rcp_neo4j_csv",
        "delimiter": ",",
        "query_dir": "neo4j/queries_cordis_test",
        "include_globs": ["*.csv"],
        "max_node_rows": 200000,
        "max_rel_rows": 200000,
    },
}


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description=(
            "Run Neo4j-backed query analysis across available partition outputs and write "
            "queries_results.{csv,md,txt}."
        )
    )
    ap.add_argument("--uri", default="bolt://localhost:7687")
    ap.add_argument("--user", default="neo4j")
    ap.add_argument("--password", default="test12345")
    ap.add_argument("--no-auth", action="store_true")
    ap.add_argument(
        "--results-csv",
        default="results/queries_results.csv",
        help="Output CSV (default: results/queries_results.csv)",
    )
    ap.add_argument(
        "--results-md",
        default="results/queries_results.md",
        help="Output Markdown summary (default: results/queries_results.md)",
    )
    ap.add_argument(
        "--results-txt",
        default="results/queries_results.txt",
        help="Output text summary (default: results/queries_results.txt)",
    )
    ap.add_argument(
        "--summary-csv",
        default="results/comparison_summary.csv",
        help="Comparison summary CSV used for case discovery.",
    )
    ap.add_argument(
        "--datasets",
        default="ldbc_inputs_1_4,fib25_neo4j_inputs,mb6_neo4j_inputs",
        help="Comma-separated datasets to include.",
    )
    ap.add_argument(
        "--algorithms",
        default="metis,kahip_fast,scotch,ptscotch,parmetis,rcp",
        help="Comma-separated algorithms to include.",
    )
    ap.add_argument("--ks", default="2,4,6,8", help="Comma-separated k values.")
    ap.add_argument("--warmup", type=int, default=0, help="Warmup iterations per query file.")
    ap.add_argument("--repeats", type=int, default=1, help="Measured repeats per query file.")
    ap.add_argument("--batch-size", type=int, default=5000, help="Batch size for CSV loading/update.")
    ap.add_argument(
        "--max-node-rows",
        type=int,
        default=0,
        help="Override max node rows loaded per dataset (0 = dataset default).",
    )
    ap.add_argument(
        "--max-rel-rows",
        type=int,
        default=0,
        help="Override max relationship rows loaded per dataset (0 = dataset default).",
    )
    ap.add_argument(
        "--max-cases",
        type=int,
        default=0,
        help="Optional cap on number of cases (0 = no cap).",
    )
    ap.add_argument(
        "--skip-load",
        action="store_true",
        help="Skip dataset loading and only run mapping/bench on existing DB content.",
    )
    return ap.parse_args()


def sanitize_symbol(s: str) -> str:
    clean = "".join(ch for ch in s if ch.isalnum() or ch == "_")
    if not clean:
        return "X"
    if clean[0].isdigit():
        clean = f"L_{clean}"
    return clean


def normalize_prop_key(col: str) -> str:
    s = col.strip()
    if ":" in s:
        s = s.split(":", 1)[0]
    s = s.strip().lstrip(":")
    if not s:
        s = col
    s = s.replace(".", "_")
    s = "".join(ch if (ch.isalnum() or ch == "_") else "_" for ch in s)
    s = re.sub(r"_+", "_", s).strip("_")
    return sanitize_symbol(s or col)


def parse_scalar(value: str):
    s = value.strip()
    if s == "":
        return None
    sl = s.lower()
    if sl == "true":
        return True
    if sl == "false":
        return False
    if INT_RE.match(s):
        try:
            return int(s)
        except Exception:
            return s
    if FLOAT_RE.match(s):
        try:
            return float(s)
        except Exception:
            return s
    return s


def p95(values: Sequence[float]) -> float:
    if not values:
        return 0.0
    s = sorted(values)
    idx = max(0, int(round(0.95 * len(s))) - 1)
    return s[idx]


def resolve_summary_path(project_dir: Path, raw_path: str) -> Optional[Path]:
    raw = (raw_path or "").strip()
    if not raw:
        return None
    p = Path(raw)
    if p.is_file():
        return p
    prefix = "/workspace/spark/"
    if raw == "/workspace/spark":
        mapped = project_dir
    elif raw.startswith(prefix):
        mapped = project_dir / raw[len(prefix) :]
    else:
        mapped = p
    return mapped.resolve()


def split_query_file(text: str) -> List[str]:
    return [b.strip() for b in text.split("\n\n") if b.strip()]


def clear_graph(driver) -> None:
    with driver.session() as s:
        while True:
            rec = s.run(
                """
                MATCH (n)
                WITH n LIMIT $chunk
                DETACH DELETE n
                RETURN count(*) AS deleted
                """,
                chunk=20000,
            ).single()
            deleted = int(rec["deleted"]) if rec else 0
            if deleted == 0:
                break


def ensure_entity_lookup(driver) -> None:
    # Query evaluation repeatedly loads synthetic :Entity nodes and then
    # matches them by id for edge load and partition assignment. Without an
    # index/constraint, MERGE/MATCH on Entity(id) degenerates badly on the
    # larger bounded loads (notably fib25/CORDIS).
    driver.execute_query(
        "CREATE CONSTRAINT entity_id_unique IF NOT EXISTS FOR (n:Entity) REQUIRE n.id IS UNIQUE"
    )


def detect_file_mode(fieldnames: List[str]) -> str:
    if not fieldnames:
        return "unknown"
    has_start = any(":START_ID" in c for c in fieldnames)
    has_end = any(":END_ID" in c for c in fieldnames)
    has_id = any(":ID" in c and ":START_ID" not in c and ":END_ID" not in c for c in fieldnames)
    if has_start and has_end:
        return "rel"
    if has_id:
        return "node"
    return "unknown"


def infer_id_col(fieldnames: List[str]) -> Optional[str]:
    for c in fieldnames:
        if ":ID" in c and ":START_ID" not in c and ":END_ID" not in c:
            return c
    return fieldnames[0] if fieldnames else None


def infer_rel_cols(fieldnames: List[str]) -> Tuple[Optional[str], Optional[str]]:
    start = None
    end = None
    for c in fieldnames:
        if ":START_ID" in c:
            start = c
        if ":END_ID" in c:
            end = c
    if start is None and fieldnames:
        start = fieldnames[0]
    if end is None and len(fieldnames) > 1:
        end = fieldnames[1]
    return start, end


def label_from_id_col(id_col: str) -> Optional[str]:
    m = re.search(r":ID\(([^)]+)\)", id_col)
    if not m:
        return None
    return sanitize_symbol(m.group(1))


def parse_label_tokens(v: str) -> List[str]:
    out: List[str] = []
    for tok in v.split(";"):
        t = sanitize_symbol(tok.strip())
        if t:
            out.append(t)
    return out


def infer_rel_type(file_name: str) -> str:
    base = file_name[:-4] if file_name.endswith(".csv") else file_name
    if "Neuron_Connections" in base:
        return "CONNECTS_TO"
    parts = base.split("_")
    if len(parts) >= 3 and parts[1] and not parts[1].isdigit():
        return sanitize_symbol(parts[1])
    return sanitize_symbol(base)


def chunked(items: Sequence[dict], size: int) -> Iterable[List[dict]]:
    if size <= 0:
        size = 1000
    chunk: List[dict] = []
    for it in items:
        chunk.append(it)
        if len(chunk) >= size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk


def load_nodes_file(
    driver, csv_path: Path, delimiter: str, batch_size: int, max_rows: int = 0
) -> Tuple[int, int]:
    with csv_path.open("r", encoding="utf-8", newline="", errors="ignore") as f:
        reader = csv.DictReader(f, delimiter=delimiter)
        fields = reader.fieldnames or []
        id_col = infer_id_col(fields)
        if id_col is None:
            return (0, 0)
        label_col = None
        for c in fields:
            if c.strip().upper() == ":LABEL":
                label_col = c
                break
        fallback_label = label_from_id_col(id_col)

        total = 0
        skipped = 0
        grouped: Dict[Tuple[str, ...], List[dict]] = defaultdict(list)
        grouped_count = 0

        def flush_groups():
            nonlocal grouped_count
            for labels, rows in list(grouped.items()):
                label_clause = "".join(f":`{lbl}`" for lbl in labels if lbl)
                q = (
                    "UNWIND $rows AS row\n"
                    "MERGE (n:Entity {id: row.id})\n"
                    f"SET n{label_clause}\n"
                    "SET n += row.props\n"
                )
                driver.execute_query(q, rows=rows)
            grouped.clear()
            grouped_count = 0

        for row in reader:
            if max_rows > 0 and total >= max_rows:
                break
            raw_id = (row.get(id_col) or "").strip()
            if raw_id == "":
                skipped += 1
                continue
            pid = parse_scalar(raw_id)
            if pid is None:
                skipped += 1
                continue

            labels: List[str] = []
            if label_col:
                labels = parse_label_tokens((row.get(label_col) or "").strip())
            if not labels and fallback_label:
                labels = [fallback_label]
            if not labels:
                labels = ["Entity"]
            labels = sorted(set(labels))

            props = {}
            for k, v in row.items():
                if k is None or k == id_col or k == label_col:
                    continue
                if v is None or v == "":
                    continue
                pv = parse_scalar(v)
                if pv is not None:
                    props[normalize_prop_key(k)] = pv
            props["id"] = pid

            grouped[tuple(labels)].append({"id": pid, "props": props})
            grouped_count += 1
            total += 1
            if grouped_count >= batch_size:
                flush_groups()

        if grouped_count > 0:
            flush_groups()

    return (total, skipped)


def load_relationships_file(
    driver,
    csv_path: Path,
    delimiter: str,
    batch_size: int,
    max_rows: int = 0,
    valid_ids: Optional[set] = None,
) -> Tuple[int, int]:
    with csv_path.open("r", encoding="utf-8", newline="", errors="ignore") as f:
        reader = csv.DictReader(f, delimiter=delimiter)
        fields = reader.fieldnames or []
        src_col, dst_col = infer_rel_cols(fields)
        if src_col is None or dst_col is None:
            return (0, 0)

        rel_type = infer_rel_type(csv_path.name)
        q = (
            "UNWIND $rows AS row\n"
            "MATCH (a:Entity {id: row.sid})\n"
            "MATCH (b:Entity {id: row.did})\n"
            f"CREATE (a)-[r:`{rel_type}`]->(b)\n"
            "SET r += row.props\n"
        )

        total = 0
        skipped = 0
        batch: List[dict] = []
        for row in reader:
            if max_rows > 0 and total >= max_rows:
                break
            sid_raw = (row.get(src_col) or "").strip()
            did_raw = (row.get(dst_col) or "").strip()
            if sid_raw == "" or did_raw == "":
                skipped += 1
                continue
            sid = parse_scalar(sid_raw)
            did = parse_scalar(did_raw)
            if sid is None or did is None:
                skipped += 1
                continue
            if valid_ids is not None:
                if sid not in valid_ids or did not in valid_ids:
                    skipped += 1
                    continue
            props = {}
            for k, v in row.items():
                if k is None or k in {src_col, dst_col}:
                    continue
                if v is None or v == "":
                    continue
                pv = parse_scalar(v)
                if pv is not None:
                    props[normalize_prop_key(k)] = pv
            batch.append({"sid": sid, "did": did, "props": props})
            total += 1
            if len(batch) >= batch_size:
                driver.execute_query(q, rows=batch)
                batch = []
        if batch:
            driver.execute_query(q, rows=batch)

    return (total, skipped)


def load_dataset_into_neo4j(
    driver,
    dataset: str,
    cfg: dict,
    project_dir: Path,
    batch_size: int,
    max_node_rows_override: int = 0,
    max_rel_rows_override: int = 0,
) -> Dict[str, float]:
    input_dir = (project_dir / cfg["input_dir"]).resolve()
    delimiter = cfg["delimiter"]
    include_globs = cfg.get("include_globs", ["*.csv"])
    max_node_rows = max_node_rows_override if max_node_rows_override > 0 else int(cfg.get("max_node_rows", 0))
    max_rel_rows = max_rel_rows_override if max_rel_rows_override > 0 else int(cfg.get("max_rel_rows", 0))
    per_file_node_cap = int(cfg.get("per_file_node_cap", 0))
    per_file_rel_cap = int(cfg.get("per_file_rel_cap", 0))
    files: List[Path] = []
    for pat in include_globs:
        files.extend(input_dir.glob(pat))
    files = sorted(set(files))
    if not files:
        raise RuntimeError(f"No CSV files found for dataset={dataset} at {input_dir}")

    clear_graph(driver)
    ensure_entity_lookup(driver)
    t0 = time.perf_counter()
    node_files = 0
    rel_files = 0
    node_rows = 0
    rel_rows = 0

    node_paths: List[Path] = []
    rel_paths: List[Path] = []
    for p in files:
        with p.open("r", encoding="utf-8", newline="", errors="ignore") as f:
            rd = csv.reader(f, delimiter=delimiter)
            try:
                header = next(rd)
            except StopIteration:
                continue
        mode = detect_file_mode(header)
        if mode == "node":
            node_paths.append(p)
        elif mode == "rel":
            rel_paths.append(p)

    for p in node_paths:
        node_files += 1
        remaining = max(0, max_node_rows - node_rows) if max_node_rows > 0 else 0
        if max_node_rows > 0 and remaining == 0:
            break
        file_cap = per_file_node_cap if per_file_node_cap > 0 else remaining
        if remaining > 0 and file_cap > 0:
            file_cap = min(file_cap, remaining)
        w, _ = load_nodes_file(driver, p, delimiter, batch_size, max_rows=file_cap)
        node_rows += w

    valid_ids = None
    rec_nodes = driver.execute_query("MATCH (n:Entity) RETURN count(n) AS c").records[0]
    node_count = int(rec_nodes["c"] or 0)
    if node_count > 0 and node_count <= 500000:
        id_rows = driver.execute_query("MATCH (n:Entity) RETURN n.id AS id").records
        valid_ids = {r["id"] for r in id_rows}

    for p in rel_paths:
        rel_files += 1
        remaining = max(0, max_rel_rows - rel_rows) if max_rel_rows > 0 else 0
        if max_rel_rows > 0 and remaining == 0:
            break
        file_cap = per_file_rel_cap if per_file_rel_cap > 0 else remaining
        if remaining > 0 and file_cap > 0:
            file_cap = min(file_cap, remaining)
        w, _ = load_relationships_file(
            driver,
            p,
            delimiter,
            batch_size,
            max_rows=file_cap,
            valid_ids=valid_ids,
        )
        rel_rows += w

    t1 = time.perf_counter()
    rec_n = driver.execute_query("MATCH (n) RETURN count(n) AS c").records[0]
    rec_r = driver.execute_query("MATCH ()-[r]->() RETURN count(r) AS c").records[0]
    return {
        "load_time_sec": t1 - t0,
        "node_files": float(node_files),
        "rel_files": float(rel_files),
        "node_rows_loaded": float(node_rows),
        "rel_rows_loaded": float(rel_rows),
        "db_nodes": float(rec_n["c"]),
        "db_rels": float(rec_r["c"]),
    }


def read_assignments(path: Path) -> List[int]:
    vals: List[int] = []
    with path.open("r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            s = line.strip()
            if s:
                vals.append(int(s))
    if not vals:
        raise RuntimeError(f"Empty assignment file: {path}")
    return vals


def read_id_to_partition(node_index_path: Path, assignments: List[int]) -> List[Tuple[object, int]]:
    out: List[Tuple[object, int]] = []
    with node_index_path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f, delimiter="\t")
        fields = reader.fieldnames or []
        idx_col = None
        id_col = None
        for c in fields:
            lc = c.lower().strip()
            if lc in {"idx", "node_idx"}:
                idx_col = c
            if lc in {"raw_id", "node_id"}:
                id_col = c
        if idx_col is None or id_col is None:
            raise RuntimeError(f"Missing idx/raw_id columns in {node_index_path}")

        for row in reader:
            ridx = (row.get(idx_col) or "").strip()
            rid = (row.get(id_col) or "").strip()
            if not ridx or not rid:
                continue
            idx = int(ridx)
            part = None
            if 0 <= idx < len(assignments):
                part = assignments[idx]
            elif 1 <= idx <= len(assignments):
                part = assignments[idx - 1]
            if part is None:
                continue
            out.append((parse_scalar(rid), int(part)))
    if not out:
        raise RuntimeError(f"No id->partition mapping from {node_index_path}")
    return out


def apply_partition_mapping(driver, mapping: List[Tuple[object, int]], batch_size: int) -> Tuple[int, int]:
    driver.execute_query("MATCH (n:Entity) REMOVE n.partition")
    matched = 0
    total = len(mapping)

    # For modest graphs, pre-filter mapping IDs to existing node IDs to avoid
    # spending most update time on guaranteed misses.
    rec_n = driver.execute_query("MATCH (n:Entity) RETURN count(n) AS c").records[0]
    n_nodes = int(rec_n["c"] or 0)
    if 0 < n_nodes <= 500000:
        id_rows = driver.execute_query("MATCH (n:Entity) RETURN n.id AS id").records
        id_set = {r["id"] for r in id_rows}
        mapping = [(i, p) for i, p in mapping if i in id_set]

    q = (
        "UNWIND $rows AS row\n"
        "MATCH (n:Entity {id: row.id})\n"
        "SET n.partition = row.partition\n"
        "RETURN count(n) AS c\n"
    )
    batch: List[dict] = []
    for node_id, part in mapping:
        batch.append({"id": node_id, "partition": part})
        if len(batch) >= batch_size:
            rec = driver.execute_query(q, rows=batch).records[0]
            matched += int(rec["c"])
            batch = []
    if batch:
        rec = driver.execute_query(q, rows=batch).records[0]
        matched += int(rec["c"])
    return matched, total


def compute_partition_metrics(driver) -> Dict[str, float]:
    rec = driver.execute_query(
        """
        CALL () {
          MATCH (n:Entity) WHERE n.partition IS NOT NULL
          RETURN count(n) AS assigned_nodes
        }
        CALL () {
          MATCH ()-[r]->()
          RETURN count(r) AS total_edges
        }
        CALL () {
          MATCH (a:Entity)-[r]->(b:Entity)
          WHERE a.partition IS NOT NULL AND b.partition IS NOT NULL
          RETURN count(r) AS assigned_edges,
                 sum(CASE WHEN a.partition <> b.partition THEN 1 ELSE 0 END) AS cross_edges
        }
        CALL () {
          MATCH (n:Entity) WHERE n.partition IS NOT NULL
          WITH n.partition AS p, count(*) AS c
          RETURN count(*) AS k_detected, min(c) AS min_block, max(c) AS max_block, avg(c) AS avg_block
        }
        RETURN assigned_nodes, total_edges, assigned_edges, cross_edges, k_detected, min_block, max_block, avg_block
        """
    ).records[0]
    assigned_edges = float(rec["assigned_edges"] or 0)
    cross_edges = float(rec["cross_edges"] or 0)
    ratio = (cross_edges / assigned_edges) if assigned_edges > 0 else 0.0
    avg_block = float(rec["avg_block"] or 0)
    max_block = float(rec["max_block"] or 0)
    balance = (max_block / avg_block) if avg_block > 0 else 0.0
    return {
        "assigned_nodes": float(rec["assigned_nodes"] or 0),
        "total_edges": float(rec["total_edges"] or 0),
        "assigned_edges": assigned_edges,
        "cross_partition_edges": cross_edges,
        "cross_partition_edge_ratio": ratio,
        "k_detected": float(rec["k_detected"] or 0),
        "min_block": float(rec["min_block"] or 0),
        "max_block": max_block,
        "partition_balance": balance,
    }


def run_query_benchmark(driver, query_dir: Path, warmup: int, repeats: int) -> Dict[str, float]:
    qfiles = sorted(glob.glob(str(query_dir / "*.txt")))
    if not qfiles:
        raise RuntimeError(f"No query files in {query_dir}")
    per_query_avg: List[float] = []
    per_query_p50: List[float] = []
    per_query_p95: List[float] = []
    all_iter: List[float] = []
    t0 = time.perf_counter()
    with driver.session() as session:
        for qf in qfiles:
            text = Path(qf).read_text(encoding="utf-8", errors="ignore")
            blocks = split_query_file(text)
            for _ in range(max(0, warmup)):
                for q in blocks:
                    session.run(q).consume()
            times: List[float] = []
            for _ in range(max(1, repeats)):
                st = time.perf_counter()
                for q in blocks:
                    session.run(q).consume()
                et = time.perf_counter()
                ms = (et - st) * 1000.0
                times.append(ms)
                all_iter.append(ms)
            per_query_avg.append(statistics.mean(times))
            per_query_p50.append(statistics.median(times))
            per_query_p95.append(p95(times))
    t1 = time.perf_counter()
    return {
        "query_files": float(len(qfiles)),
        "query_batch_exec_time_sec": t1 - t0,
        "query_avg_ms": statistics.mean(per_query_avg) if per_query_avg else 0.0,
        "query_p50_ms": statistics.median(per_query_p50) if per_query_p50 else 0.0,
        "query_p95_ms": statistics.median(per_query_p95) if per_query_p95 else 0.0,
        "query_std_ms": statistics.pstdev(all_iter) if len(all_iter) > 1 else 0.0,
    }


def discover_cases(project_dir: Path, summary_csv: Path, datasets: set, algorithms: set, ks: set) -> List[dict]:
    rows = []
    with summary_csv.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            run_label = row.get("run_label", "")
            m = RUN_LABEL_RE.match(run_label)
            if not m:
                continue
            ds = m.group("dataset")
            alg = m.group("algorithm")
            k = int(m.group("k"))
            if ds not in datasets or alg not in algorithms or k not in ks:
                continue
            # Prefer exact paths from comparison summary rows. This works for both
            # old local results and newer Docker/native result layouts.
            part_file_raw = row.get("output_partition_file") or ""
            node_index_raw = row.get("node_index_file") or ""
            part_file = resolve_summary_path(project_dir, part_file_raw)
            node_index = resolve_summary_path(project_dir, node_index_raw)

            if part_file is None or not part_file.is_file() or node_index is None or not node_index.is_file():
                run_dir = project_dir / "results" / f"{alg}_{ds}_k{k}"
                part_name = PARTITION_FILE_TEMPLATE[alg].format(k=k)
                part_file = run_dir / part_name
                node_index = run_dir / "node_index.tsv"
            if not part_file.is_file() or not node_index.is_file():
                continue
            rows.append(
                {
                    "dataset": ds,
                    "algorithm": alg,
                    "k": k,
                    "run_label": run_label,
                    "part_file": str(part_file.resolve()),
                    "node_index_file": str(node_index.resolve()),
                }
            )
    rows.sort(key=lambda r: (r["dataset"], r["k"], r["algorithm"]))
    return rows


def write_csv(path: Path, rows: List[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        with path.open("w", encoding="utf-8", newline="") as f:
            f.write("")
        return
    fields = list(rows[0].keys())
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(rows)


def build_summary_text(rows: List[dict]) -> str:
    if not rows:
        return "No rows."

    lines: List[str] = []
    lines.append(f"Generated: {datetime.now(timezone.utc).isoformat()}")
    lines.append("")

    by_ds_k: Dict[Tuple[str, int], List[dict]] = defaultdict(list)
    for r in rows:
        by_ds_k[(r["dataset"], int(r["k"]))].append(r)

    for ds, k in sorted(by_ds_k.keys()):
        lines.append(f"## {ds} k={k}")
        cand = by_ds_k[(ds, k)]
        cand_sorted = sorted(cand, key=lambda x: float(x["cross_partition_edge_ratio"]))
        best = cand_sorted[0]
        lines.append(
            f"Best hop ratio: {best['algorithm']} ({best['cross_partition_edge_ratio']})"
        )
        for r in cand_sorted:
            lines.append(
                f"- {r['algorithm']}: cross_ratio={r['cross_partition_edge_ratio']}, "
                f"query_avg_ms={r['query_avg_ms']}, query_p95_ms={r['query_p95_ms']}, "
                f"mapping_coverage={r['mapping_coverage']}"
            )
        lines.append("")

    lines.append("## Global Notes")
    lines.append("- Lower cross_partition_edge_ratio is better for partition locality.")
    lines.append("- Query latency on one machine may vary less than partition locality metrics.")
    return "\n".join(lines).strip() + "\n"


def main() -> int:
    args = parse_args()
    project_dir = Path(__file__).resolve().parent.parent
    summary_csv = (project_dir / args.summary_csv).resolve()
    out_csv = (project_dir / args.results_csv).resolve()
    out_md = (project_dir / args.results_md).resolve()
    out_txt = (project_dir / args.results_txt).resolve()

    datasets = {x.strip() for x in args.datasets.split(",") if x.strip()}
    algorithms = {x.strip() for x in args.algorithms.split(",") if x.strip()}
    ks = {int(x.strip()) for x in args.ks.split(",") if x.strip()}

    cases = discover_cases(project_dir, summary_csv, datasets, algorithms, ks)
    if args.max_cases > 0:
        cases = cases[: args.max_cases]
    if not cases:
        raise RuntimeError("No cases discovered from comparison summary.")

    auth = None if args.no_auth else (args.user, args.password)
    driver = GraphDatabase.driver(args.uri, auth=auth)

    case_rows: List[dict] = []
    dataset_load_cache: Dict[str, Dict[str, float]] = {}
    try:
        for i, case in enumerate(cases, 1):
            ds = case["dataset"]
            alg = case["algorithm"]
            k = case["k"]
            case_label = f"{alg}_{ds}_k{k}"
            print(f"[CASE {i}/{len(cases)}] {case_label}")

            cfg = DATASET_CONFIG.get(ds)
            if cfg is None:
                print(f"[WARN] Skipping unsupported dataset config: {ds}")
                continue

            if ds not in dataset_load_cache:
                if args.skip_load:
                    dataset_load_cache[ds] = {
                        "load_time_sec": 0.0,
                        "node_files": 0.0,
                        "rel_files": 0.0,
                        "node_rows_loaded": 0.0,
                        "rel_rows_loaded": 0.0,
                        "db_nodes": 0.0,
                        "db_rels": 0.0,
                    }
                else:
                    print(f"[INFO] Loading dataset into Neo4j: {ds}")
                    dataset_load_cache[ds] = load_dataset_into_neo4j(
                        driver=driver,
                        dataset=ds,
                        cfg=cfg,
                        project_dir=project_dir,
                        batch_size=args.batch_size,
                        max_node_rows_override=args.max_node_rows,
                        max_rel_rows_override=args.max_rel_rows,
                    )
                    print(
                        "[INFO] Loaded dataset "
                        f"{ds} in {dataset_load_cache[ds]['load_time_sec']:.2f}s "
                        f"(nodes={dataset_load_cache[ds]['db_nodes']:.0f}, rels={dataset_load_cache[ds]['db_rels']:.0f})"
                    )

            assignments = read_assignments(Path(case["part_file"]))
            id_to_part = read_id_to_partition(Path(case["node_index_file"]), assignments)
            matched, total = apply_partition_mapping(driver, id_to_part, args.batch_size)

            part_metrics = compute_partition_metrics(driver)
            q_metrics = run_query_benchmark(
                driver=driver,
                query_dir=(project_dir / cfg["query_dir"]).resolve(),
                warmup=args.warmup,
                repeats=args.repeats,
            )

            load_meta = dataset_load_cache[ds]
            row = {
                "timestamp_utc": datetime.now(timezone.utc).isoformat(),
                "dataset": ds,
                "algorithm": alg,
                "k": k,
                "case_label": case_label,
                "partition_file": case["part_file"],
                "node_index_file": case["node_index_file"],
                "mapping_total": total,
                "mapping_matched": matched,
                "mapping_coverage": round((matched / total), 6) if total > 0 else 0.0,
                "assigned_nodes": int(part_metrics["assigned_nodes"]),
                "total_edges_db": int(part_metrics["total_edges"]),
                "assigned_edges": int(part_metrics["assigned_edges"]),
                "cross_partition_edges": int(part_metrics["cross_partition_edges"]),
                "cross_partition_edge_ratio": round(part_metrics["cross_partition_edge_ratio"], 6),
                "k_detected": int(part_metrics["k_detected"]),
                "min_block": int(part_metrics["min_block"]),
                "max_block": int(part_metrics["max_block"]),
                "partition_balance": round(part_metrics["partition_balance"], 6),
                "query_files": int(q_metrics["query_files"]),
                "query_exec_time_sec": round(q_metrics["query_batch_exec_time_sec"], 6),
                "query_avg_ms": round(q_metrics["query_avg_ms"], 6),
                "query_p50_ms": round(q_metrics["query_p50_ms"], 6),
                "query_p95_ms": round(q_metrics["query_p95_ms"], 6),
                "query_std_ms": round(q_metrics["query_std_ms"], 6),
                "dataset_load_time_sec": round(load_meta["load_time_sec"], 6),
                "dataset_loaded_nodes": int(load_meta["db_nodes"]),
                "dataset_loaded_rels": int(load_meta["db_rels"]),
            }
            case_rows.append(row)
    finally:
        driver.close()

    write_csv(out_csv, case_rows)
    summary_text = build_summary_text(case_rows)
    out_md.parent.mkdir(parents=True, exist_ok=True)
    out_md.write_text(summary_text, encoding="utf-8")
    out_txt.parent.mkdir(parents=True, exist_ok=True)
    out_txt.write_text(summary_text, encoding="utf-8")

    print(f"[OK] Wrote CSV: {out_csv}")
    print(f"[OK] Wrote MD:  {out_md}")
    print(f"[OK] Wrote TXT: {out_txt}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
