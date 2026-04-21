#!/usr/bin/env python3
import argparse
import csv
import zipfile
from collections import defaultdict
from pathlib import Path


def parse_args():
    ap = argparse.ArgumentParser(
        description="Unpack CORDIS Horizon CSV zips and build partition inputs (graph.metis, node_index.tsv, hash_nodes.txt)."
    )
    ap.add_argument(
        "--raw-zip-dir",
        default="datasets/external_raw/cordis_horizon",
        help="Directory containing CORDIS zip files.",
    )
    ap.add_argument(
        "--dataset-dir",
        default="datasets/cordis_horizon_inputs",
        help="Output dataset directory under the repository root.",
    )
    return ap.parse_args()


def extract_zip(zip_path: Path, out_dir: Path) -> None:
    if not zip_path.is_file():
        raise RuntimeError(f"zip not found: {zip_path}")
    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(out_dir)


def norm(v):
    if v is None:
        return ""
    return str(v).strip().strip('"')


def read_csv_rows(path: Path):
    with path.open("r", encoding="utf-8", errors="ignore", newline="") as f:
        r = csv.DictReader(f, delimiter=";", quotechar='"')
        for row in r:
            yield row


def main():
    args = parse_args()

    script_dir = Path(__file__).resolve().parent
    project_dir = script_dir.parent.parent

    raw_zip_dir = (project_dir / args.raw_zip_dir).resolve()
    dataset_dir = (project_dir / args.dataset_dir).resolve()
    raw_csv_dir = dataset_dir / "raw_csv"
    prep_dir = dataset_dir / "partition_prep"
    work_dir = prep_dir / "work"
    dummy_rel_dir = prep_dir / "hash_relationships_dummy"

    raw_csv_dir.mkdir(parents=True, exist_ok=True)
    work_dir.mkdir(parents=True, exist_ok=True)
    dummy_rel_dir.mkdir(parents=True, exist_ok=True)

    zip_projects = raw_zip_dir / "cordis-HORIZONprojects-csv.zip"
    zip_publications = raw_zip_dir / "cordis-HORIZONprojectPublications-csv.zip"
    zip_deliverables = raw_zip_dir / "cordis-HORIZONprojectDeliverables-csv.zip"

    project_csv = raw_csv_dir / "project.csv"
    publication_csv = raw_csv_dir / "projectPublications.csv"
    deliverable_csv = raw_csv_dir / "projectDeliverables.csv"

    if not project_csv.is_file():
        print(f"[INFO] Extracting {zip_projects.name}")
        extract_zip(zip_projects, raw_csv_dir)
    if not publication_csv.is_file():
        print(f"[INFO] Extracting {zip_publications.name}")
        extract_zip(zip_publications, raw_csv_dir)
    if not deliverable_csv.is_file():
        print(f"[INFO] Extracting {zip_deliverables.name}")
        extract_zip(zip_deliverables, raw_csv_dir)

    if not project_csv.is_file():
        raise RuntimeError(f"missing expected file after unzip: {project_csv}")
    if not publication_csv.is_file():
        raise RuntimeError(f"missing expected file after unzip: {publication_csv}")
    if not deliverable_csv.is_file():
        raise RuntimeError(f"missing expected file after unzip: {deliverable_csv}")

    project_ids = set()
    publication_ids = set()
    deliverable_ids = set()

    edges_project_pub = set()
    edges_project_deliv = set()

    print("[INFO] Reading project.csv")
    for row in read_csv_rows(project_csv):
        pid = norm(row.get("id"))
        if pid:
            project_ids.add(pid)

    print("[INFO] Reading projectPublications.csv")
    for row in read_csv_rows(publication_csv):
        pub_id = norm(row.get("id"))
        pid = norm(row.get("projectID"))
        if not pub_id or not pid:
            continue
        publication_ids.add(pub_id)
        project_ids.add(pid)
        edges_project_pub.add((pid, pub_id))

    print("[INFO] Reading projectDeliverables.csv")
    for row in read_csv_rows(deliverable_csv):
        did = norm(row.get("id"))
        pid = norm(row.get("projectID"))
        if not did or not pid:
            continue
        deliverable_ids.add(did)
        project_ids.add(pid)
        edges_project_deliv.add((pid, did))

    if not project_ids:
        raise RuntimeError("No project IDs found.")

    node_keys = []
    for pid in sorted(project_ids):
        node_keys.append(("Project", pid))
    for pub_id in sorted(publication_ids):
        node_keys.append(("Publication", pub_id))
    for did in sorted(deliverable_ids):
        node_keys.append(("Deliverable", did))

    idx_of = {key: i + 1 for i, key in enumerate(node_keys)}
    total_nodes = len(node_keys)
    adjacency = defaultdict(set)

    for pid, pub_id in edges_project_pub:
        u = idx_of[("Project", pid)]
        v = idx_of[("Publication", pub_id)]
        if u != v:
            adjacency[u].add(v)
            adjacency[v].add(u)

    for pid, did in edges_project_deliv:
        u = idx_of[("Project", pid)]
        v = idx_of[("Deliverable", did)]
        if u != v:
            adjacency[u].add(v)
            adjacency[v].add(u)

    undirected_edges = sum(len(vs) for vs in adjacency.values()) // 2

    graph_path = work_dir / "graph.metis"
    node_index_path = prep_dir / "node_index.tsv"
    hash_nodes_path = prep_dir / "hash_nodes.txt"

    with graph_path.open("w", encoding="utf-8", newline="") as f:
        f.write(f"{total_nodes} {undirected_edges}\n")
        for i in range(1, total_nodes + 1):
            nbrs = sorted(adjacency.get(i, ()))
            if nbrs:
                f.write(" ".join(str(x) for x in nbrs))
            f.write("\n")

    with node_index_path.open("w", encoding="utf-8", newline="") as f:
        f.write("idx\ttype\traw_id\n")
        for i, (typ, rid) in enumerate(node_keys, start=1):
            f.write(f"{i}\t{typ}\t{rid}\n")

    with hash_nodes_path.open("w", encoding="utf-8", newline="") as f:
        for i in range(1, total_nodes + 1):
            f.write(f"id:{i}\n")

    print(f"[OK] dataset-dir: {dataset_dir}")
    print(f"[OK] graph.metis: {graph_path}")
    print(f"[OK] node_index: {node_index_path}")
    print(f"[OK] hash_nodes: {hash_nodes_path}")
    print(f"[OK] hash_dummy_relationships_dir: {dummy_rel_dir}")
    print(
        "[OK] counts: "
        f"projects={len(project_ids)} publications={len(publication_ids)} "
        f"deliverables={len(deliverable_ids)} nodes={total_nodes} edges={undirected_edges}"
    )


if __name__ == "__main__":
    main()
