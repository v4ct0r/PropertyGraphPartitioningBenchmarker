#!/usr/bin/env python3
import argparse
import csv
from pathlib import Path

from neo4j import GraphDatabase


def write_nodes(session, out_dir: Path) -> None:
    out_path = out_dir / "Node.csv"
    query = "MATCH (n) RETURN id(n) AS gid ORDER BY gid"
    count = 0
    with out_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, delimiter="|", lineterminator="\n")
        writer.writerow(["gid:ID(Node)"])
        for record in session.run(query):
            writer.writerow([record["gid"]])
            count += 1
    print(f"[OK] Node: {count} rows -> {out_path}")


def write_relationships(session, out_dir: Path) -> None:
    out_path = out_dir / "CONNECTED.csv"
    query = "MATCH (a)-[r]->(b) RETURN id(a) AS src, id(b) AS dst"
    count = 0
    with out_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, delimiter="|", lineterminator="\n")
        writer.writerow(["Node.gid:START_ID(Node)", "Node.gid:END_ID(Node)"])
        for record in session.run(query):
            writer.writerow([record["src"], record["dst"]])
            count += 1
    print(f"[OK] CONNECTED: {count} rows -> {out_path}")


def main() -> int:
    ap = argparse.ArgumentParser(description="Export full CORD-19 graph from Neo4j into a structural Neo4j-style CSV dataset.")
    ap.add_argument("--host", default="localhost")
    ap.add_argument("--port", type=int, default=7689)
    ap.add_argument("--user", default="neo4j")
    ap.add_argument("--password", default="test12345")
    ap.add_argument("--out-dir", default="datasets/cord19_full_structural_inputs")
    args = ap.parse_args()

    out_dir = Path(args.out_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    driver = GraphDatabase.driver(f"bolt://{args.host}:{args.port}", auth=(args.user, args.password))
    try:
        with driver.session() as session:
            write_nodes(session, out_dir)
            write_relationships(session, out_dir)
    finally:
        driver.close()

    print(f"[OK] Export completed: {out_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
