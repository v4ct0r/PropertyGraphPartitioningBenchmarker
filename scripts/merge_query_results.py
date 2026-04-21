#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path

PROJECT_DIR = Path(__file__).resolve().parent.parent
if str(PROJECT_DIR) not in sys.path:
    sys.path.insert(0, str(PROJECT_DIR))

from scripts.run_neo4j_queries_results import build_summary_text, write_csv


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description="Merge query result CSVs into a single deduplicated queries_results.{csv,md,txt} set."
    )
    ap.add_argument(
        "--base-csv",
        default="results/queries_results.csv",
        help="Base query results CSV to merge into (default: results/queries_results.csv)",
    )
    ap.add_argument(
        "--inputs",
        nargs="+",
        required=True,
        help="Additional query result CSVs to merge.",
    )
    ap.add_argument(
        "--out-csv",
        default="results/queries_results.csv",
        help="Merged output CSV (default: results/queries_results.csv)",
    )
    ap.add_argument(
        "--out-md",
        default="results/queries_results.md",
        help="Merged output Markdown summary (default: results/queries_results.md)",
    )
    ap.add_argument(
        "--out-txt",
        default="results/queries_results.txt",
        help="Merged output text summary (default: results/queries_results.txt)",
    )
    return ap.parse_args()


def read_rows(path: Path) -> list[dict]:
    if not path.exists() or path.stat().st_size == 0:
        return []
    with path.open(newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def key_for(row: dict) -> tuple[str, str, int]:
    return (row["dataset"], row["algorithm"], int(row["k"]))


def main() -> int:
    args = parse_args()
    project_dir = PROJECT_DIR

    base_csv = (project_dir / args.base_csv).resolve()
    input_paths = [(project_dir / p).resolve() for p in args.inputs]
    out_csv = (project_dir / args.out_csv).resolve()
    out_md = (project_dir / args.out_md).resolve()
    out_txt = (project_dir / args.out_txt).resolve()

    merged_by_key: dict[tuple[str, str, int], dict] = {}

    for row in read_rows(base_csv):
        merged_by_key[key_for(row)] = row

    for path in input_paths:
        for row in read_rows(path):
            merged_by_key[key_for(row)] = row

    merged_rows = sorted(merged_by_key.values(), key=lambda r: (r["dataset"], int(r["k"]), r["algorithm"]))
    write_csv(out_csv, merged_rows)
    summary = build_summary_text(merged_rows)
    out_md.parent.mkdir(parents=True, exist_ok=True)
    out_md.write_text(summary, encoding="utf-8")
    out_txt.parent.mkdir(parents=True, exist_ok=True)
    out_txt.write_text(summary, encoding="utf-8")

    print(f"[OK] Merged rows: {len(merged_rows)}")
    print(f"[OK] CSV: {out_csv}")
    print(f"[OK] MD:  {out_md}")
    print(f"[OK] TXT: {out_txt}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
