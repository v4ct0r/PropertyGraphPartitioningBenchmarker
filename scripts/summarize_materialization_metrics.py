#!/usr/bin/env python3
from __future__ import annotations

import csv
import re
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


REPO_ROOT = Path(__file__).resolve().parents[1]
RESULTS_DIR = REPO_ROOT / "results"
LOGS_DIR = RESULTS_DIR / "logs"
ALGORITHMS = ("kahip_fast", "parmetis", "ptscotch", "scotch", "metis", "rcp")


@dataclass
class MaterializedRun:
    dataset: str
    algorithm: str
    k: int
    docker_root: str
    materialized_path: Path
    bytes_total: int
    files_total: int
    partitions_total: int
    max_partition_bytes: int
    avg_partition_bytes: float
    rows_written: int | None
    rows_skipped: int | None
    materialization_time_sec: float | None
    log_file: str | None


def parse_run_name(run_name: str) -> tuple[str, str, int] | None:
    for algorithm in sorted(ALGORITHMS, key=len, reverse=True):
        prefix = f"{algorithm}_"
        if not run_name.startswith(prefix):
            continue
        rest = run_name[len(prefix) :]
        match = re.match(r"(.+)_k(\d+)$", rest)
        if not match:
            return None
        dataset = match.group(1)
        return dataset, algorithm, int(match.group(2))
    return None


def latest_logs_by_key() -> dict[tuple[str, str, int], Path]:
    grouped: dict[tuple[str, str, int], list[Path]] = defaultdict(list)
    log_paths = list(LOGS_DIR.glob("materialize_*.log"))
    log_paths.extend(RESULTS_DIR.glob("docker*/logs/*.log"))
    for log_path in log_paths:
        stem = log_path.name.removesuffix(".log")
        match = re.match(r"materialize_(.+)_k(\d+)_\d{4}-\d{2}-\d{2}_\d{6}$", stem)
        if match:
            dataset_and_algorithm, k_text = match.groups()
            for algorithm in sorted(ALGORITHMS, key=len, reverse=True):
                suffix = f"_{algorithm}"
                if not dataset_and_algorithm.endswith(suffix):
                    continue
                dataset = dataset_and_algorithm[: -len(suffix)]
                grouped[(dataset, algorithm, int(k_text))].append(log_path)
                break
            continue
        match = re.match(r"([a-z0-9_]+)_(.+)_k(\d+)$", stem)
        if match:
            algorithm, dataset, k_text = match.groups()
            if algorithm in ALGORITHMS:
                grouped[(dataset, algorithm, int(k_text))].append(log_path)
    return {key: sorted(paths)[-1] for key, paths in grouped.items()}


def parse_log_metrics(log_path: Path) -> tuple[int | None, int | None, float | None]:
    rows_written = None
    rows_skipped = None
    elapsed = None
    try:
        text = log_path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return rows_written, rows_skipped, elapsed
    if match := re.search(r"\[OK\] Total rows written: (\d+)", text):
        rows_written = int(match.group(1))
    if match := re.search(r"\[OK\] Total rows skipped: (\d+)", text):
        rows_skipped = int(match.group(1))
    if match := re.search(r"\[OK\] Elapsed sec: ([0-9.]+)", text):
        elapsed = float(match.group(1))
    return rows_written, rows_skipped, elapsed


def summarize_dir(path: Path) -> tuple[int, int, int, float]:
    total_bytes = 0
    total_files = 0
    partition_sizes: list[int] = []
    for partition_dir in sorted(p for p in path.iterdir() if p.is_dir()):
        part_bytes = 0
        for file_path in partition_dir.rglob("*"):
            if file_path.is_file():
                total_files += 1
                size = file_path.stat().st_size
                total_bytes += size
                part_bytes += size
        partition_sizes.append(part_bytes)
    max_partition_bytes = max(partition_sizes) if partition_sizes else 0
    avg_partition_bytes = (sum(partition_sizes) / len(partition_sizes)) if partition_sizes else 0.0
    return total_bytes, total_files, len(partition_sizes), max_partition_bytes, avg_partition_bytes


def iter_materialized_runs() -> Iterable[MaterializedRun]:
    logs = latest_logs_by_key()
    for path in sorted(RESULTS_DIR.glob("docker*/**/materialized_property_graph")):
        if not path.is_dir():
            continue
        docker_root = next((part for part in path.parts if part.startswith("docker")), None)
        if not docker_root:
            continue
        run_name = path.parent.name
        parsed = parse_run_name(run_name)
        if not parsed:
            continue
        dataset, algorithm, k = parsed
        total_bytes, total_files, partitions_total, max_partition_bytes, avg_partition_bytes = summarize_dir(path)
        log_path = logs.get((dataset, algorithm, k))
        rows_written, rows_skipped, elapsed = parse_log_metrics(log_path) if log_path else (None, None, None)
        yield MaterializedRun(
            dataset=dataset,
            algorithm=algorithm,
            k=k,
            docker_root=docker_root,
            materialized_path=path,
            bytes_total=total_bytes,
            files_total=total_files,
            partitions_total=partitions_total,
            max_partition_bytes=max_partition_bytes,
            avg_partition_bytes=avg_partition_bytes,
            rows_written=rows_written,
            rows_skipped=rows_skipped,
            materialization_time_sec=elapsed,
            log_file=str(log_path.relative_to(REPO_ROOT)) if log_path else None,
        )


def write_csv(rows: list[MaterializedRun], path: Path) -> None:
    fieldnames = [
        "dataset",
        "algorithm",
        "k",
        "docker_root",
        "materialized_path",
        "bytes_total",
        "files_total",
        "partitions_total",
        "max_partition_bytes",
        "avg_partition_bytes",
        "rows_written",
        "rows_skipped",
        "materialization_time_sec",
        "log_file",
    ]
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {
                    "dataset": row.dataset,
                    "algorithm": row.algorithm,
                    "k": row.k,
                    "docker_root": row.docker_root,
                    "materialized_path": str(row.materialized_path.relative_to(REPO_ROOT)),
                    "bytes_total": row.bytes_total,
                    "files_total": row.files_total,
                    "partitions_total": row.partitions_total,
                    "max_partition_bytes": row.max_partition_bytes,
                    "avg_partition_bytes": f"{row.avg_partition_bytes:.2f}",
                    "rows_written": row.rows_written if row.rows_written is not None else "",
                    "rows_skipped": row.rows_skipped if row.rows_skipped is not None else "",
                    "materialization_time_sec": (
                        f"{row.materialization_time_sec:.3f}" if row.materialization_time_sec is not None else ""
                    ),
                    "log_file": row.log_file or "",
                }
            )


def write_markdown(rows: list[MaterializedRun], path: Path) -> None:
    lines = [
        "# Materialization Metrics",
        "",
        "| dataset | algorithm | k | bytes_total | files_total | partitions_total | rows_written | materialization_time_sec | docker_root |",
        "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |",
    ]
    for row in rows:
        lines.append(
            f"| {row.dataset} | {row.algorithm} | {row.k} | {row.bytes_total} | {row.files_total} | "
            f"{row.partitions_total} | {row.rows_written or ''} | "
            f"{'' if row.materialization_time_sec is None else f'{row.materialization_time_sec:.3f}'} | {row.docker_root} |"
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    rows = sorted(iter_materialized_runs(), key=lambda r: (r.dataset, r.algorithm, r.k, r.materialized_path))
    csv_path = RESULTS_DIR / "materialization_metrics.csv"
    md_path = RESULTS_DIR / "materialization_metrics.md"
    write_csv(rows, csv_path)
    write_markdown(rows, md_path)
    print(f"Wrote {len(rows)} rows to {csv_path}")
    print(f"Wrote {md_path}")


if __name__ == "__main__":
    main()
