#!/usr/bin/env python3
import argparse
import csv
import subprocess
from pathlib import Path
from typing import Dict, List, Optional, Tuple

ID_RE = r"(?i)^:?(?:[^:]+:)?ID(?:\(([^)]+)\))?$"
START_RE = r"(?i)^:?(?:[^:]+:)?START_ID(?:\(([^)]+)\))?$"
END_RE = r"(?i)^:?(?:[^:]+:)?END_ID(?:\(([^)]+)\))?$"


def run(cmd: List[str]) -> None:
    print("[RUN]", " ".join(cmd))
    subprocess.run(cmd, check=True)


def detect_delimiter(header_line: str) -> str:
    # Neo4j import files are usually comma-separated, but we support pipe as fallback.
    if "|" in header_line and header_line.count("|") >= header_line.count(","):
        return "|"
    return ","


def parse_header(path: Path) -> Tuple[List[str], str]:
    with path.open("r", encoding="utf-8", errors="ignore", newline="") as f:
        first = f.readline()
        if not first:
            return [], ","
        delim = detect_delimiter(first)
        header = next(csv.reader([first], delimiter=delim))
        return [h.strip() for h in header], delim


def _match(regex: str, text: str):
    import re
    return re.match(regex, text.strip())


def detect_node_file(header: List[str], filename: str) -> Optional[Tuple[int, str]]:
    for i, col in enumerate(header):
        m = _match(ID_RE, col)
        if m:
            id_type = (m.group(1) or Path(filename).stem).strip() or "Node"
            return i, id_type
    return None


def detect_edge_file(header: List[str]) -> Optional[Tuple[int, str, int, str]]:
    s_idx = e_idx = None
    s_type = e_type = ""
    for i, col in enumerate(header):
        ms = _match(START_RE, col)
        if ms:
            s_idx = i
            s_type = (ms.group(1) or "Node").strip() or "Node"
        me = _match(END_RE, col)
        if me:
            e_idx = i
            e_type = (me.group(1) or "Node").strip() or "Node"
    if s_idx is None or e_idx is None:
        return None
    return s_idx, s_type, e_idx, e_type


def count_lines(path: Path) -> int:
    n = 0
    with path.open("r", encoding="utf-8", errors="ignore") as f:
        for _ in f:
            n += 1
    return n


def main() -> None:
    ap = argparse.ArgumentParser(description="Prepare Neo4j CSV folder for native partition algorithms.")
    ap.add_argument("--input-dir", required=True, help="Directory containing Neo4j CSV files")
    ap.add_argument("--out-dir", required=True, help="Output working directory")
    args = ap.parse_args()

    input_dir = Path(args.input_dir).resolve()
    out_dir = Path(args.out_dir).resolve()
    temp_dir = out_dir / "temp"
    work_dir = out_dir / "work"
    hash_rel_dir = out_dir / "hash_relationships_dummy"
    node_index_tsv = out_dir / "node_index.tsv"
    hash_nodes_txt = out_dir / "hash_nodes.txt"
    raw_edges = temp_dir / "edges_raw.tsv"
    uniq_edges = temp_dir / "edges_uniq.tsv"
    directed_sorted = temp_dir / "edges_directed_sorted.tsv"
    metis_graph = work_dir / "graph.metis"

    if not input_dir.is_dir():
        raise RuntimeError(f"input-dir not found: {input_dir}")

    out_dir.mkdir(parents=True, exist_ok=True)
    temp_dir.mkdir(parents=True, exist_ok=True)
    work_dir.mkdir(parents=True, exist_ok=True)
    hash_rel_dir.mkdir(parents=True, exist_ok=True)

    csv_files = sorted([p for p in input_dir.glob("*.csv") if p.is_file()])
    if not csv_files:
        raise RuntimeError(f"No CSV files found in: {input_dir}")

    node_specs = []  # (path, delim, id_col, id_type)
    edge_specs = []  # (path, delim, s_idx, s_type, e_idx, e_type)
    ignored = []

    print("[INFO] Scanning CSV headers...")
    for p in csv_files:
        header, delim = parse_header(p)
        if not header:
            ignored.append(p)
            continue

        edge_info = detect_edge_file(header)
        if edge_info is not None:
            s_idx, s_type, e_idx, e_type = edge_info
            edge_specs.append((p, delim, s_idx, s_type, e_idx, e_type))
            continue

        node_info = detect_node_file(header, p.name)
        if node_info is not None:
            id_col, id_type = node_info
            node_specs.append((p, delim, id_col, id_type))
            continue

        ignored.append(p)

    print(f"[INFO] Node files: {len(node_specs)}")
    print(f"[INFO] Edge files: {len(edge_specs)}")
    print(f"[INFO] Ignored files: {len(ignored)}")

    node_map: Dict[Tuple[str, str], int] = {}
    next_idx = 1

    def ensure_node(node_type: str, raw_id: str) -> int:
        nonlocal next_idx
        key = (node_type, raw_id)
        idx = node_map.get(key)
        if idx is None:
            idx = next_idx
            node_map[key] = idx
            next_idx += 1
        return idx

    for p, delim, id_col, id_type in node_specs:
        print(f"[NODES] {p.name} (id_type={id_type})")
        with p.open("r", encoding="utf-8", errors="ignore", newline="") as f:
            r = csv.reader(f, delimiter=delim)
            try:
                next(r)
            except StopIteration:
                continue
            for row in r:
                if id_col >= len(row):
                    continue
                raw_id = row[id_col].strip()
                if raw_id == "":
                    continue
                ensure_node(id_type, raw_id)

    with raw_edges.open("w", encoding="utf-8") as ew:
        for p, delim, s_idx, s_type, e_idx, e_type in edge_specs:
            print(f"[EDGES] {p.name} ({s_type} -> {e_type})")
            with p.open("r", encoding="utf-8", errors="ignore", newline="") as f:
                r = csv.reader(f, delimiter=delim)
                try:
                    next(r)
                except StopIteration:
                    continue
                for row in r:
                    if s_idx >= len(row) or e_idx >= len(row):
                        continue
                    sid = row[s_idx].strip()
                    eid = row[e_idx].strip()
                    if sid == "" or eid == "":
                        continue

                    u = ensure_node(s_type, sid)
                    v = ensure_node(e_type, eid)
                    if u == v:
                        continue
                    a, b = (u, v) if u < v else (v, u)
                    ew.write(f"{a}\t{b}\n")

    total_nodes = next_idx - 1
    if total_nodes <= 0:
        raise RuntimeError("No nodes found while preparing dataset.")
    print(f"[INFO] Total indexed nodes: {total_nodes}")

    with node_index_tsv.open("w", encoding="utf-8", newline="") as w:
        w.write("idx\ttype\traw_id\n")
        inv: List[Optional[Tuple[str, str]]] = [None] * total_nodes
        for (t, raw_id), idx in node_map.items():
            inv[idx - 1] = (t, raw_id)
        for i, item in enumerate(inv, start=1):
            t, raw_id = item if item is not None else ("Unknown", str(i))
            w.write(f"{i}\t{t}\t{raw_id}\n")

    # Synthetic nodes file for the legacy hash-compatible parser (expects id:<num>).
    with hash_nodes_txt.open("w", encoding="utf-8", newline="") as w:
        for i in range(1, total_nodes + 1):
            w.write(f"id:{i}\n")

    run([
        "bash",
        "-lc",
        f"LC_ALL=C sort -T '{temp_dir}' -S 50% -u -n -k1,1 -k2,2 '{raw_edges}' > '{uniq_edges}'",
    ])
    undirected_count = count_lines(uniq_edges)
    print(f"[INFO] Unique undirected edges: {undirected_count}")

    run([
        "bash",
        "-lc",
        (
            f"awk '{{print $1 \"\\t\" $2; print $2 \"\\t\" $1}}' '{uniq_edges}' "
            f"| LC_ALL=C sort -T '{temp_dir}' -S 50% -n -k1,1 -k2,2 > '{directed_sorted}'"
        ),
    ])

    with metis_graph.open("w", encoding="utf-8", newline="") as out:
        out.write(f"{total_nodes} {undirected_count}\n")
        current_node = 1
        prev_u = None
        neighbors: List[int] = []

        def flush_until(target_u: int) -> None:
            nonlocal current_node
            while current_node < target_u:
                out.write("\n")
                current_node += 1

        def flush_prev() -> None:
            nonlocal prev_u, neighbors, current_node
            if prev_u is None:
                return
            flush_until(prev_u)
            out.write(" ".join(str(x) for x in neighbors) + "\n")
            current_node = prev_u + 1

        with directed_sorted.open("r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                s = line.strip()
                if not s:
                    continue
                cols = s.split("\t")
                if len(cols) != 2:
                    continue
                u = int(cols[0])
                v = int(cols[1])
                if prev_u is None:
                    prev_u = u
                    neighbors = [v]
                    continue
                if u == prev_u:
                    neighbors.append(v)
                    continue
                flush_prev()
                prev_u = u
                neighbors = [v]

        flush_prev()
        while current_node <= total_nodes:
            out.write("\n")
            current_node += 1

    print(f"[OK] Prepared METIS graph: {metis_graph}")
    print(f"[OK] Prepared node index:  {node_index_tsv}")
    print(f"[OK] Prepared hash nodes:  {hash_nodes_txt}")
    print(f"[OK] Hash dummy rel dir:    {hash_rel_dir}")


if __name__ == "__main__":
    main()
