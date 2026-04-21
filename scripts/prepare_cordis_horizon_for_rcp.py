#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
from pathlib import Path


def parse_args():
    ap = argparse.ArgumentParser(
        description="Build a Neo4j-style CSV projection for CORDIS Horizon matching the graph.metis topology used by the other partitioners."
    )
    ap.add_argument(
        "--dataset-dir",
        default="datasets/cordis_horizon_inputs",
        help="CORDIS dataset directory containing raw_csv/ and partition_prep/",
    )
    ap.add_argument(
        "--out-dir",
        default="",
        help="Output Neo4j-style CSV dir (default: <dataset-dir>/rcp_neo4j_csv)",
    )
    return ap.parse_args()


def norm(v):
    if v is None:
        return ""
    return str(v).strip().strip('"')


def read_csv_rows(path: Path):
    with path.open("r", encoding="utf-8", errors="ignore", newline="") as f:
        r = csv.DictReader(f, delimiter=";", quotechar='"')
        for row in r:
            yield row


def write_csv(path: Path, header, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f, lineterminator="\n")
        w.writerow(header)
        for row in rows:
            w.writerow(row)


def main():
    args = parse_args()
    project_dir = Path(__file__).resolve().parent.parent
    dataset_dir = (project_dir / args.dataset_dir).resolve()
    out_dir = (Path(args.out_dir).resolve() if args.out_dir else (dataset_dir / "rcp_neo4j_csv").resolve())
    raw_csv_dir = dataset_dir / "raw_csv"

    project_csv = raw_csv_dir / "project.csv"
    publication_csv = raw_csv_dir / "projectPublications.csv"
    deliverable_csv = raw_csv_dir / "projectDeliverables.csv"
    if not project_csv.is_file() or not publication_csv.is_file() or not deliverable_csv.is_file():
        raise RuntimeError(f"Expected CORDIS raw_csv files under {raw_csv_dir}")

    projects = {}
    publications = {}
    deliverables = {}
    project_pub = set()
    project_deliv = set()

    for row in read_csv_rows(project_csv):
        pid = norm(row.get("id"))
        if not pid:
            continue
        projects[pid] = {
            "acronym": norm(row.get("acronym")),
            "title": norm(row.get("title")),
            "status": norm(row.get("status")),
        }

    for row in read_csv_rows(publication_csv):
        pub_id = norm(row.get("id"))
        pid = norm(row.get("projectID"))
        if not pub_id or not pid:
            continue
        publications[pub_id] = {
            "title": norm(row.get("title")),
            "doi": norm(row.get("doi")),
            "journalTitle": norm(row.get("journalTitle")),
        }
        project_pub.add((pid, pub_id))
        if pid not in projects:
            projects[pid] = {"acronym": "", "title": "", "status": ""}

    for row in read_csv_rows(deliverable_csv):
        did = norm(row.get("id"))
        pid = norm(row.get("projectID"))
        if not did or not pid:
            continue
        deliverables[did] = {
            "title": norm(row.get("title")),
            "deliverableType": norm(row.get("deliverableType")),
        }
        project_deliv.add((pid, did))
        if pid not in projects:
            projects[pid] = {"acronym": "", "title": "", "status": ""}

    write_csv(
        out_dir / "Project.csv",
        ["project_id:ID(Project)", "acronym", "title", "status"],
        ([pid, info["acronym"], info["title"], info["status"]] for pid, info in sorted(projects.items())),
    )
    write_csv(
        out_dir / "Publication.csv",
        ["publication_id:ID(Publication)", "title", "doi", "journalTitle"],
        ([pub_id, info["title"], info["doi"], info["journalTitle"]] for pub_id, info in sorted(publications.items())),
    )
    write_csv(
        out_dir / "Deliverable.csv",
        ["deliverable_id:ID(Deliverable)", "title", "deliverableType"],
        ([did, info["title"], info["deliverableType"]] for did, info in sorted(deliverables.items())),
    )
    write_csv(
        out_dir / "PROJECT_PUBLICATION.csv",
        [":START_ID(Project)", ":END_ID(Publication)"],
        (list(edge) for edge in sorted(project_pub)),
    )
    write_csv(
        out_dir / "PROJECT_DELIVERABLE.csv",
        [":START_ID(Project)", ":END_ID(Deliverable)"],
        (list(edge) for edge in sorted(project_deliv)),
    )

    print(f"[OK] dataset-dir: {dataset_dir}")
    print(f"[OK] out-dir: {out_dir}")
    print(f"[OK] projects={len(projects)} publications={len(publications)} deliverables={len(deliverables)}")
    print(f"[OK] project-publication edges={len(project_pub)}")
    print(f"[OK] project-deliverable edges={len(project_deliv)}")


if __name__ == "__main__":
    main()
