#!/usr/bin/env python3
import argparse
import csv
from pathlib import Path


def main() -> None:
    ap = argparse.ArgumentParser(description="Merge multiple comparison_summary.csv files into one aggregate CSV.")
    ap.add_argument("--output", required=True, help="Output aggregate CSV path")
    ap.add_argument("--inputs", nargs="*", help="Explicit input CSV files")
    ap.add_argument(
        "--glob",
        default="results/docker*/comparison_summary.csv",
        help="Glob used when --inputs is omitted (default: results/docker*/comparison_summary.csv)",
    )
    args = ap.parse_args()

    output = Path(args.output).resolve()
    if args.inputs:
        input_paths = [Path(p).resolve() for p in args.inputs]
    else:
        input_paths = [p.resolve() for p in sorted(Path.cwd().glob(args.glob))]

    input_paths = [p for p in input_paths if p.is_file() and p != output]
    if not input_paths:
        raise SystemExit("No input comparison_summary.csv files found.")

    ordered = sorted(input_paths, key=lambda p: (p.stat().st_mtime_ns, str(p)))
    fieldnames = None
    by_run_label = {}

    for path in ordered:
        with path.open("r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            if reader.fieldnames is None:
                continue
            if fieldnames is None:
                fieldnames = reader.fieldnames
            for row in reader:
                run_label = row.get("run_label", "").strip()
                if run_label:
                    by_run_label[run_label] = row

    if fieldnames is None:
        raise SystemExit("Could not determine CSV fieldnames from inputs.")

    rows = sorted(by_run_label.values(), key=lambda row: (row.get("run_label", ""), row.get("algorithm", ""), row.get("k", "")))
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"[OK] Merged {len(ordered)} files into {output}")
    print(f"[OK] Rows written: {len(rows)}")


if __name__ == "__main__":
    main()
