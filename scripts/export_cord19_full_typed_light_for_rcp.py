#!/usr/bin/env python3
import argparse
import csv
from pathlib import Path

from neo4j import GraphDatabase


def quote_ident(name: str) -> str:
    return f"`{name.replace('`', '``')}`"


def get_labels(session):
    rows = session.run("MATCH (n) RETURN DISTINCT labels(n) AS ls")
    labels = []
    for row in rows:
        ls = row["ls"]
        if len(ls) != 1:
            raise RuntimeError(f"Expected single-label nodes, found label combination: {ls}")
        labels.append(ls[0])
    return sorted(labels)


def get_property_count(session, label: str) -> int:
    query = f"MATCH (n:{quote_ident(label)}) UNWIND keys(n) AS k RETURN count(DISTINCT k) AS c"
    row = session.run(query).single()
    return int(row["c"] or 0)


def get_rel_specs(session):
    rel_types = [row[0] for row in session.run("CALL db.relationshipTypes()")]
    specs = []
    for rel_type in sorted(rel_types):
        query = (
            f"MATCH (a)-[r:{quote_ident(rel_type)}]->(b) "
            "RETURN DISTINCT head(labels(a)) AS src_label, head(labels(b)) AS dst_label "
            "ORDER BY src_label, dst_label"
        )
        for row in session.run(query):
            specs.append((rel_type, row["src_label"], row["dst_label"]))
    return specs


def write_nodes(session, out_dir: Path, label: str, property_count: int) -> None:
    out_path = out_dir / f"{label}.csv"
    dummy_props = [f"prop_{i}" for i in range(1, property_count + 1)]
    header = [f"gid:ID({label})"] + dummy_props
    query = f"MATCH (n:{quote_ident(label)}) RETURN id(n) AS gid ORDER BY gid"
    blanks = [""] * property_count
    count = 0
    with out_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, delimiter="|", lineterminator="\n")
        writer.writerow(header)
        for record in session.run(query):
            writer.writerow([record["gid"], *blanks])
            count += 1
    print(f"[OK] Node {label}: {count} rows, {property_count} props -> {out_path}")


def write_relationships(session, out_dir: Path, rel_type: str, src_label: str, dst_label: str) -> None:
    out_path = out_dir / f"{src_label}_{rel_type}_{dst_label}.csv"
    header = [f"{src_label}.gid:START_ID({src_label})", f"{dst_label}.gid:END_ID({dst_label})"]
    query = (
        f"MATCH (a:{quote_ident(src_label)})-[r:{quote_ident(rel_type)}]->(b:{quote_ident(dst_label)}) "
        "RETURN id(a) AS src, id(b) AS dst"
    )
    count = 0
    with out_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, delimiter="|", lineterminator="\n")
        writer.writerow(header)
        for record in session.run(query):
            writer.writerow([record["src"], record["dst"]])
            count += 1
    print(f"[OK] Relationship {rel_type} ({src_label}->{dst_label}): {count} rows -> {out_path}")


def main() -> int:
    ap = argparse.ArgumentParser(description="Export full typed CORD-19 graph in a lightweight form suitable for RCP.")
    ap.add_argument("--host", default="localhost")
    ap.add_argument("--port", type=int, default=7689)
    ap.add_argument("--user", default="neo4j")
    ap.add_argument("--password", default="test12345")
    ap.add_argument("--out-dir", default="datasets/cord19_full_typed_light_inputs")
    args = ap.parse_args()

    out_dir = Path(args.out_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    driver = GraphDatabase.driver(f"bolt://{args.host}:{args.port}", auth=(args.user, args.password))
    try:
        with driver.session() as session:
            labels = get_labels(session)
            print(f"[INFO] labels: {len(labels)} -> {labels}")
            for label in labels:
                prop_count = get_property_count(session, label)
                write_nodes(session, out_dir, label, prop_count)
            rel_specs = get_rel_specs(session)
            print(f"[INFO] relationship specs: {len(rel_specs)}")
            for rel_type, src_label, dst_label in rel_specs:
                write_relationships(session, out_dir, rel_type, src_label, dst_label)
    finally:
        driver.close()

    print(f"[OK] Export completed: {out_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
