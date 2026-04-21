#!/usr/bin/env python3
import argparse
import csv
from pathlib import Path

from neo4j import GraphDatabase


LABELS = {
    "Paper": ["_hash_id", "cord_uid", "source", "url", "title", "journal", "publish_time", "cord19-fulltext_hash"],
    "Author": ["_hash_id", "last", "suffix", "first", "email", "middle"],
    "Affiliation": ["_hash_id", "laboratory", "institution"],
    "Location": ["_hash_id", "settlement", "country", "region"],
    "Reference": ["_hash_id", "title", "name", "pages", "year", "ref_id", "volume", "venue"],
    "PaperID": ["id", "type"],
}

RELATIONSHIPS = [
    ("PAPER_HAS_PAPERID", "Paper", "_hash_id", "PaperID", "id"),
    ("REFERENCE_HAS_PAPERID", "Reference", "_hash_id", "PaperID", "id"),
    ("AUTHOR_HAS_AFFILIATION", "Author", "_hash_id", "Affiliation", "_hash_id"),
    ("AFFILIATION_HAS_LOCATION", "Affiliation", "_hash_id", "Location", "_hash_id"),
]


def write_nodes(session, out_dir: Path, label: str, props: list[str]) -> None:
    id_key = props[0]
    out_path = out_dir / f"{label}.csv"
    header = [f"{id_key}:ID({label})"] + props[1:]
    query = f"MATCH (n:{label}) RETURN " + ", ".join([f"n.`{p}` AS `{p}`" for p in props])
    count = 0
    with out_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, delimiter="|", lineterminator="\n")
        writer.writerow(header)
        for record in session.run(query):
            row = [record[p] if record[p] is not None else "" for p in props]
            writer.writerow(row)
            count += 1
    print(f"[OK] {label}: {count} rows -> {out_path}")


def write_relationships(session, out_dir: Path, rel_type: str, src_label: str, src_key: str, dst_label: str, dst_key: str) -> None:
    out_path = out_dir / f"{rel_type}.csv"
    header = [f"{src_label}.{src_key}:START_ID({src_label})", f"{dst_label}.{dst_key}:END_ID({dst_label})"]
    query = (
        f"MATCH (a:{src_label})-[r:{rel_type}]->(b:{dst_label}) "
        f"RETURN a.`{src_key}` AS src, b.`{dst_key}` AS dst"
    )
    count = 0
    with out_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, delimiter="|", lineterminator="\n")
        writer.writerow(header)
        for record in session.run(query):
            writer.writerow([
                record["src"] if record["src"] is not None else "",
                record["dst"] if record["dst"] is not None else "",
            ])
            count += 1
    print(f"[OK] {rel_type}: {count} rows -> {out_path}")


def main() -> int:
    ap = argparse.ArgumentParser(description="Export reduced CORD-19 graph from Neo4j into Neo4j-style CSV files.")
    ap.add_argument("--host", default="localhost")
    ap.add_argument("--port", type=int, default=7689)
    ap.add_argument("--user", default="neo4j")
    ap.add_argument("--password", default="test12345")
    ap.add_argument("--out-dir", default="datasets/cord19_reduced_neo4j_inputs")
    args = ap.parse_args()

    out_dir = Path(args.out_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    driver = GraphDatabase.driver(f"bolt://{args.host}:{args.port}", auth=(args.user, args.password))
    try:
        with driver.session() as session:
            for label, props in LABELS.items():
                write_nodes(session, out_dir, label, props)
            for rel_spec in RELATIONSHIPS:
                write_relationships(session, out_dir, *rel_spec)
    finally:
        driver.close()

    print(f"[OK] Export completed: {out_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
