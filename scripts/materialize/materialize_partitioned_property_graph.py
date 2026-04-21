#!/usr/bin/env python3
import argparse
import csv
import re
import shutil
import time
from collections import defaultdict
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set, Tuple

ID_RE = r"(?i)^:?(?:[^:]+:)?ID(?:\(([^)]+)\))?$"
START_RE = r"(?i)^:?(?:[^:]+:)?START_ID(?:\(([^)]+)\))?$"
END_RE = r"(?i)^:?(?:[^:]+:)?END_ID(?:\(([^)]+)\))?$"


def detect_delimiter(header_line: str) -> str:
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


def read_assignments(path: Path) -> List[int]:
    parts: List[int] = []
    with path.open("r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            s = line.strip()
            if s:
                parts.append(int(s))
    if not parts:
        raise RuntimeError(f"No assignments found in: {path}")
    return parts


def _read_node_index_rows(node_index_path: Path) -> List[Tuple[int, str, str]]:
    rows: List[Tuple[int, str, str]] = []
    with node_index_path.open("r", encoding="utf-8", newline="") as f:
        r = csv.DictReader(f, delimiter="\t")
        if r.fieldnames is None:
            raise RuntimeError(f"Invalid node index file (no header): {node_index_path}")
        fields = {c.lower().strip(): c for c in r.fieldnames}
        idx_col = fields.get("idx") or fields.get("node_idx")
        type_col = fields.get("type")
        raw_id_col = fields.get("raw_id") or fields.get("node_id")
        if idx_col is None or type_col is None or raw_id_col is None:
            raise RuntimeError(
                f"Expected idx/type/raw_id columns in node index: {node_index_path}"
            )
        for row in r:
            raw_idx = row.get(idx_col, "").strip()
            node_type = row.get(type_col, "").strip()
            raw_id = row.get(raw_id_col, "").strip()
            if not raw_idx or not node_type or not raw_id:
                continue
            rows.append((int(raw_idx), node_type, raw_id))
    if not rows:
        raise RuntimeError(f"No usable rows found in node index: {node_index_path}")
    return rows


def build_node_partition_map(node_index_path: Path, assignments: List[int], k: int) -> Dict[Tuple[str, str], Set[int]]:
    mapping: Dict[Tuple[str, str], Set[int]] = {}
    for idx_val, node_type, raw_id in _read_node_index_rows(node_index_path):
        if 1 <= idx_val <= len(assignments):
            part = assignments[idx_val - 1]
        elif 0 <= idx_val < len(assignments):
            part = assignments[idx_val]
        else:
            continue
        if not (0 <= part < k):
            raise RuntimeError(f"Partition id out of range: idx={idx_val}, part={part}, k={k}")
        mapping[(node_type, raw_id)] = {part}
    if not mapping:
        raise RuntimeError(f"No node mapping built from: {node_index_path}")
    return mapping


def read_memberships(path: Path, k: int) -> Dict[int, Set[int]]:
    memberships: Dict[int, Set[int]] = defaultdict(set)
    current_partition: Optional[int] = None
    with path.open("r", encoding="utf-8", errors="ignore") as f:
        for raw_line in f:
            line = raw_line.strip()
            if not line:
                continue
            if line.startswith("partition_id:"):
                current_partition = int(line.split(":", 1)[1].strip())
                if not (0 <= current_partition < k):
                    raise RuntimeError(f"Partition id out of range in memberships file: {current_partition}")
                continue
            if line.startswith("partition_node_element:"):
                if current_partition is None:
                    raise RuntimeError(f"partition_node_element found before partition_id in: {path}")
                payload = line.split(":", 1)[1].strip()
                if not payload:
                    continue
                for token in payload.split(","):
                    token = token.strip()
                    if not token:
                        continue
                    memberships[int(token)].add(current_partition)
    if not memberships:
        raise RuntimeError(f"No memberships parsed from: {path}")
    return memberships


def build_node_partition_map_from_memberships(
    node_index_path: Path, membership_path: Path, k: int
) -> Dict[Tuple[str, str], Set[int]]:
    memberships = read_memberships(membership_path, k)
    mapping: Dict[Tuple[str, str], Set[int]] = {}
    for idx_val, node_type, raw_id in _read_node_index_rows(node_index_path):
        parts = memberships.get(idx_val)
        if parts is None:
            parts = memberships.get(idx_val - 1)
        if not parts:
            continue
        mapping[(node_type, raw_id)] = set(parts)
    if not mapping:
        raise RuntimeError(
            f"No node mapping built from memberships {membership_path} and node index {node_index_path}"
        )
    return mapping


def clear_output_root(out_root: Path) -> None:
    if out_root.exists():
        shutil.rmtree(out_root)


def open_partition_writers(out_root: Path, rel_path: str, header: List[str], delimiter: str, k: int):
    writers = {}
    handles = {}
    for p in range(k):
        out_path = out_root / f"partition_{p}" / rel_path
        out_path.parent.mkdir(parents=True, exist_ok=True)
        f = out_path.open("w", encoding="utf-8", newline="")
        w = csv.writer(f, delimiter=delimiter, lineterminator="\n")
        w.writerow(header)
        handles[p] = f
        writers[p] = w
    return handles, writers


def materialize_generic_file(path: Path, out_root: Path, k: int, delimiter: str, header: List[str]) -> Tuple[int, int]:
    handles, writers = open_partition_writers(out_root, path.name, header, delimiter, k)
    written = 0
    try:
        with path.open("r", encoding="utf-8", errors="ignore", newline="") as f:
            reader = csv.reader(f, delimiter=delimiter)
            next(reader, None)
            for row in reader:
                if not row:
                    continue
                for p in range(k):
                    writers[p].writerow(row)
                    written += 1
    finally:
        for f in handles.values():
            f.close()
    return written, 0


def materialize_node_file(
    path: Path,
    out_root: Path,
    k: int,
    delimiter: str,
    header: List[str],
    id_col: int,
    id_type: str,
    node_to_part: Dict[Tuple[str, str], Set[int]],
) -> Tuple[int, int]:
    handles, writers = open_partition_writers(out_root, path.name, header, delimiter, k)
    written = 0
    skipped = 0
    try:
        with path.open("r", encoding="utf-8", errors="ignore", newline="") as f:
            reader = csv.reader(f, delimiter=delimiter)
            next(reader, None)
            for row in reader:
                if id_col >= len(row):
                    skipped += 1
                    continue
                raw_id = row[id_col].strip()
                parts = node_to_part.get((id_type, raw_id))
                if not parts:
                    skipped += 1
                    continue
                for part in parts:
                    writers[part].writerow(row)
                    written += 1
    finally:
        for f in handles.values():
            f.close()
    return written, skipped


def materialize_edge_file(
    path: Path,
    out_root: Path,
    k: int,
    delimiter: str,
    header: List[str],
    s_idx: int,
    s_type: str,
    e_idx: int,
    e_type: str,
    node_to_part: Dict[Tuple[str, str], Set[int]],
    edge_mode: str,
) -> Tuple[int, int]:
    handles, writers = open_partition_writers(out_root, path.name, header, delimiter, k)
    written = 0
    skipped = 0
    try:
        with path.open("r", encoding="utf-8", errors="ignore", newline="") as f:
            reader = csv.reader(f, delimiter=delimiter)
            next(reader, None)
            for row in reader:
                if s_idx >= len(row) or e_idx >= len(row):
                    skipped += 1
                    continue
                sid = row[s_idx].strip()
                eid = row[e_idx].strip()
                src_parts = node_to_part.get((s_type, sid))
                dst_parts = node_to_part.get((e_type, eid))
                if not src_parts or not dst_parts:
                    skipped += 1
                    continue
                if edge_mode == "duplicate-cross":
                    targets = set(src_parts) | set(dst_parts)
                else:
                    targets = set(src_parts)
                for part in targets:
                    writers[part].writerow(row)
                    written += 1
    finally:
        for f in handles.values():
            f.close()
    return written, skipped


def iter_csv_files(input_dir: Path) -> Iterable[Path]:
    return sorted(p for p in input_dir.glob("*.csv") if p.is_file())


def main() -> None:
    ap = argparse.ArgumentParser(description="Materialize partitioned property-graph CSV folders from assignment output.")
    ap.add_argument("--input-dir", required=True, help="Directory containing original Neo4j CSV files")
    ap.add_argument("--node-index", required=True, help="Path to node_index.tsv")
    ap.add_argument("--assignment", required=True, help="Path to partition assignment file")
    ap.add_argument(
        "--memberships-file",
        help="Optional multi-membership file (e.g. RCP region_node_component_<k>_v2.txt) for duplicate-aware materialization",
    )
    ap.add_argument("--k", required=True, type=int, help="Partition count")
    ap.add_argument("--out-root", required=True, help="Output root for partition_0..partition_k-1")
    ap.add_argument(
        "--edge-mode",
        choices=("source", "duplicate-cross"),
        default="source",
        help="How to place relationship rows. Default: source",
    )
    ap.add_argument("--clean", action="store_true", help="Remove out-root before writing")
    args = ap.parse_args()

    input_dir = Path(args.input_dir).resolve()
    node_index = Path(args.node_index).resolve()
    assignment = Path(args.assignment).resolve()
    memberships_file = Path(args.memberships_file).resolve() if args.memberships_file else None
    out_root = Path(args.out_root).resolve()
    if not input_dir.is_dir():
        raise RuntimeError(f"input dir not found: {input_dir}")
    if not node_index.is_file():
        raise RuntimeError(f"node index not found: {node_index}")
    if not assignment.is_file():
        raise RuntimeError(f"assignment not found: {assignment}")
    if memberships_file is not None and not memberships_file.is_file():
        raise RuntimeError(f"memberships file not found: {memberships_file}")
    if args.k < 2:
        raise RuntimeError("--k must be >= 2")

    start = time.perf_counter()
    if args.clean:
        clear_output_root(out_root)
    out_root.mkdir(parents=True, exist_ok=True)
    for p in range(args.k):
        (out_root / f"partition_{p}").mkdir(parents=True, exist_ok=True)

    if memberships_file is not None:
        node_to_part = build_node_partition_map_from_memberships(node_index, memberships_file, args.k)
    else:
        assignments = read_assignments(assignment)
        node_to_part = build_node_partition_map(node_index, assignments, args.k)

    files_processed = 0
    total_written = 0
    total_skipped = 0
    for csv_path in iter_csv_files(input_dir):
        header, delimiter = parse_header(csv_path)
        if not header:
            continue
        edge_info = detect_edge_file(header)
        node_info = detect_node_file(header, csv_path.name)
        if edge_info is not None:
            s_idx, s_type, e_idx, e_type = edge_info
            written, skipped = materialize_edge_file(
                csv_path, out_root, args.k, delimiter, header, s_idx, s_type, e_idx, e_type, node_to_part, args.edge_mode
            )
            kind = f"edge {s_type}->{e_type}"
        elif node_info is not None:
            id_col, id_type = node_info
            written, skipped = materialize_node_file(
                csv_path, out_root, args.k, delimiter, header, id_col, id_type, node_to_part
            )
            kind = f"node {id_type}"
        else:
            written, skipped = materialize_generic_file(csv_path, out_root, args.k, delimiter, header)
            kind = "generic copy-to-all"
        files_processed += 1
        total_written += written
        total_skipped += skipped
        print(f"[OK] {csv_path.name}: {kind}, written={written}, skipped={skipped}")

    elapsed = time.perf_counter() - start
    print(f"[OK] Output root: {out_root}")
    print(f"[OK] Files processed: {files_processed}")
    print(f"[OK] Total rows written: {total_written}")
    print(f"[OK] Total rows skipped: {total_skipped}")
    print(f"[OK] Elapsed sec: {elapsed:.3f}")


if __name__ == "__main__":
    main()
