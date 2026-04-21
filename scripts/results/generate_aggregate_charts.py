#!/usr/bin/env python3
from __future__ import annotations

import csv
import math
from collections import defaultdict
from pathlib import Path
from statistics import mean

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
import xlsxwriter


ROOT = Path(__file__).resolve().parents[2]
RESULTS_DIR = ROOT / "results"
OUT_DIR = RESULTS_DIR / "charts"
PNG_DIR = OUT_DIR / "png"
XLSX_DIR = OUT_DIR / "xlsx"

PARTITION_CSV = RESULTS_DIR / "docker_comparison_summary.csv"
QUERY_CSV = RESULTS_DIR / "queries_results.csv"
MATERIALIZATION_CSV = RESULTS_DIR / "materialization_metrics.csv"

ALGO_ORDER = ["hash", "kahip_fast", "metis", "parmetis", "ptscotch", "scotch", "rcp"]
ALGO_LABELS = {
    "hash": "Hash",
    "kahip_fast": "KaHIP",
    "metis": "METIS",
    "parmetis": "ParMETIS",
    "ptscotch": "PT-Scotch",
    "scotch": "Scotch",
    "rcp": "RCP",
}
ALGO_COLORS = {
    "hash": "#7f8c8d",
    "kahip_fast": "#1f77b4",
    "metis": "#d62728",
    "parmetis": "#2ca02c",
    "ptscotch": "#ff7f0e",
    "scotch": "#8c564b",
    "rcp": "#9467bd",
}
ALGO_MARKERS = {
    "hash": "o",
    "kahip_fast": "s",
    "metis": "^",
    "parmetis": "D",
    "ptscotch": "P",
    "scotch": "X",
    "rcp": "*",
}
DATASET_ORDER = [
    "fib25_neo4j_inputs",
    "mb6_neo4j_inputs",
    "ldbc_inputs_1_4",
    "cordis_horizon_inputs",
    "cord19_reduced_neo4j_inputs",
    "cord19_full_structural_inputs",
    "cord19_full_typed_light_inputs",
]
DATASET_LABELS = {
    "fib25_neo4j_inputs": "FIB25",
    "mb6_neo4j_inputs": "MB6",
    "ldbc_inputs_1_4": "LDBC",
    "cordis_horizon_inputs": "CORDIS",
    "cord19_reduced_neo4j_inputs": "CORD-19 Reduced",
    "cord19_full_structural_inputs": "CORD-19 Full Structural",
    "cord19_full_typed_light_inputs": "CORD-19 Full Typed Light",
}
DATASET_MARKERS = {
    "fib25_neo4j_inputs": "o",
    "mb6_neo4j_inputs": "s",
    "ldbc_inputs_1_4": "^",
    "cordis_horizon_inputs": "D",
    "cord19_reduced_neo4j_inputs": "P",
    "cord19_full_structural_inputs": "X",
    "cord19_full_typed_light_inputs": "*",
}


def to_number(value: str) -> float:
    if value is None or value == "":
        return math.nan
    try:
        return float(value)
    except ValueError:
        return math.nan


def ensure_dirs() -> None:
    PNG_DIR.mkdir(parents=True, exist_ok=True)
    XLSX_DIR.mkdir(parents=True, exist_ok=True)


def parse_partition_dataset(row: dict[str, str]) -> str:
    label = row["run_label"]
    suffix = f"_{row['algorithm']}_k{row['k']}"
    if label.endswith(suffix):
        return label[: -len(suffix)]
    return label


def load_partition_rows() -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    with PARTITION_CSV.open(newline="", encoding="utf-8") as f:
        for raw in csv.DictReader(f):
            row: dict[str, object] = dict(raw)
            row["dataset"] = parse_partition_dataset(raw)
            row["k"] = int(raw["k"])
            for key in [
                "io_time_sec",
                "time_partition_sec",
                "graph_nodes",
                "graph_edges",
                "cut",
                "finalobjective",
                "bnd",
                "balance",
                "max_comm_vol",
            ]:
                row[key] = to_number(raw[key])
            rows.append(row)
    return rows


def load_query_rows() -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    with QUERY_CSV.open(newline="", encoding="utf-8") as f:
        for raw in csv.DictReader(f):
            row: dict[str, object] = dict(raw)
            row["k"] = int(raw["k"])
            for key in [
                "mapping_coverage",
                "cross_partition_edges",
                "cross_partition_edge_ratio",
                "partition_balance",
                "query_exec_time_sec",
                "query_avg_ms",
                "query_p50_ms",
                "query_p95_ms",
                "query_std_ms",
                "dataset_load_time_sec",
                "dataset_loaded_nodes",
                "dataset_loaded_rels",
            ]:
                row[key] = to_number(raw[key])
            rows.append(row)
    return rows


def load_materialization_rows() -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    with MATERIALIZATION_CSV.open(newline="", encoding="utf-8") as f:
        for raw in csv.DictReader(f):
            row: dict[str, object] = dict(raw)
            row["k"] = int(raw["k"])
            for key in [
                "bytes_total",
                "files_total",
                "partitions_total",
                "max_partition_bytes",
                "avg_partition_bytes",
                "rows_written",
                "rows_skipped",
                "materialization_time_sec",
            ]:
                row[key] = to_number(raw[key])
            rows.append(row)
    return rows


def metric_lookup(rows: list[dict[str, object]], metric: str) -> dict[tuple[str, str, int], float]:
    data: dict[tuple[str, str, int], float] = {}
    for row in rows:
        value = row.get(metric)
        if isinstance(value, float) and math.isnan(value):
            continue
        data[(str(row["dataset"]), str(row["algorithm"]), int(row["k"]))] = float(value)
    return data


def datasets_for(rows: list[dict[str, object]]) -> list[str]:
    present = {str(r["dataset"]) for r in rows}
    ordered = [d for d in DATASET_ORDER if d in present]
    extras = sorted(present - set(DATASET_ORDER))
    return ordered + extras


def algorithms_for(rows: list[dict[str, object]]) -> list[str]:
    present = {str(r["algorithm"]) for r in rows}
    ordered = [a for a in ALGO_ORDER if a in present]
    extras = sorted(present - set(ALGO_ORDER))
    return ordered + extras


def sanitize_sheet_name(name: str) -> str:
    bad = "[]:*?/\\"
    for ch in bad:
        name = name.replace(ch, "_")
    return name[:31]


def build_dataset_pivot(
    rows: list[dict[str, object]],
    metric: str,
    dataset_order: list[str],
    algo_order: list[str],
) -> dict[str, dict[str, dict[int, float]]]:
    pivot: dict[str, dict[str, dict[int, float]]] = {
        dataset: {algo: {} for algo in algo_order} for dataset in dataset_order
    }
    for row in rows:
        dataset = str(row["dataset"])
        algo = str(row["algorithm"])
        if dataset not in pivot:
            continue
        value = row.get(metric)
        if isinstance(value, float) and math.isnan(value):
            continue
        if algo in pivot[dataset]:
            pivot[dataset][algo][int(row["k"])] = float(value)
    return pivot


def plot_dataset_grid(
    *,
    rows: list[dict[str, object]],
    metric: str,
    title: str,
    ylabel: str,
    png_name: str,
    workbook_name: str,
    description: str,
    log_y: bool = False,
) -> None:
    dataset_order = datasets_for(rows)
    algo_order = algorithms_for(rows)
    pivot = build_dataset_pivot(rows, metric, dataset_order, algo_order)

    cols = 3
    n = len(dataset_order)
    rows_count = math.ceil(n / cols)
    fig, axes = plt.subplots(rows_count, cols, figsize=(18, 4.5 * rows_count), squeeze=False)
    fig.suptitle(title, fontsize=18, fontweight="bold")

    for idx, dataset in enumerate(dataset_order):
        ax = axes[idx // cols][idx % cols]
        dataset_has_only_positive = True
        for algo in algo_order:
            points = sorted(pivot[dataset][algo].items())
            if not points:
                continue
            xs = [k for k, _ in points]
            ys = [v for _, v in points]
            if any(v <= 0 for v in ys):
                dataset_has_only_positive = False
            ax.plot(
                xs,
                ys,
                label=ALGO_LABELS.get(algo, algo),
                color=ALGO_COLORS.get(algo, "#000000"),
                marker=ALGO_MARKERS.get(algo, "o"),
                linewidth=2,
                markersize=6,
            )
        ax.set_title(DATASET_LABELS.get(dataset, dataset), fontsize=11)
        ax.set_xlabel("k")
        ax.set_ylabel(ylabel)
        ax.set_xticks(sorted({k for algo in algo_order for k in pivot[dataset][algo].keys()}))
        if log_y and dataset_has_only_positive:
            ax.set_yscale("log")
        ax.grid(True, alpha=0.25)

    for idx in range(n, rows_count * cols):
        axes[idx // cols][idx % cols].axis("off")

    legend_handles = [
        Line2D(
            [0],
            [0],
            color=ALGO_COLORS.get(algo, "#000000"),
            marker=ALGO_MARKERS.get(algo, "o"),
            linewidth=2,
            label=ALGO_LABELS.get(algo, algo),
        )
        for algo in algo_order
    ]
    fig.legend(handles=legend_handles, loc="lower center", ncol=min(len(legend_handles), 7), frameon=False)
    fig.tight_layout(rect=[0, 0.06, 1, 0.95])
    fig.savefig(PNG_DIR / png_name, dpi=180, bbox_inches="tight")
    plt.close(fig)

    workbook = xlsxwriter.Workbook(str(XLSX_DIR / workbook_name))
    fmt_title = workbook.add_format({"bold": True, "font_size": 14})
    fmt_header = workbook.add_format({"bold": True, "bg_color": "#D9E1F2", "border": 1})
    fmt_num = workbook.add_format({"num_format": "0.000"})
    fmt_int = workbook.add_format({"num_format": "0"})
    fmt_desc = workbook.add_format({"text_wrap": True, "valign": "top"})

    summary = workbook.add_worksheet("README")
    summary.write("A1", title, fmt_title)
    summary.write("A3", description, fmt_desc)
    summary.write("A6", "Metric", fmt_header)
    summary.write("B6", metric)
    summary.set_column("A:A", 18)
    summary.set_column("B:B", 70)

    for dataset in dataset_order:
        sheet = workbook.add_worksheet(sanitize_sheet_name(DATASET_LABELS.get(dataset, dataset)))
        sheet.write_row(0, 0, ["k"] + [ALGO_LABELS.get(a, a) for a in algo_order], fmt_header)
        ks = sorted({k for algo in algo_order for k in pivot[dataset][algo].keys()})
        for row_idx, k in enumerate(ks, start=1):
            sheet.write_number(row_idx, 0, k, fmt_int)
            for col_idx, algo in enumerate(algo_order, start=1):
                value = pivot[dataset][algo].get(k)
                if value is not None:
                    sheet.write_number(row_idx, col_idx, value, fmt_num)
        dataset_has_only_positive = all(
            value > 0
            for algo in algo_order
            for value in pivot[dataset][algo].values()
        ) if ks else False
        chart = workbook.add_chart({"type": "line"})
        for col_idx, algo in enumerate(algo_order, start=1):
            if not pivot[dataset][algo]:
                continue
            chart.add_series(
                {
                    "name": [sheet.name, 0, col_idx],
                    "categories": [sheet.name, 1, 0, len(ks), 0],
                    "values": [sheet.name, 1, col_idx, len(ks), col_idx],
                    "line": {"color": ALGO_COLORS.get(algo, "#000000"), "width": 2.25},
                    "marker": {"type": "circle", "size": 6, "border": {"color": ALGO_COLORS.get(algo, "#000000")}, "fill": {"color": ALGO_COLORS.get(algo, "#000000")}},
                }
            )
        chart.set_title({"name": f"{DATASET_LABELS.get(dataset, dataset)} - {ylabel}"})
        chart.set_x_axis({"name": "k", "major_gridlines": {"visible": False}})
        y_axis = {"name": ylabel}
        if log_y and dataset_has_only_positive:
            y_axis["log_base"] = 10
        chart.set_y_axis(y_axis)
        chart.set_legend({"position": "bottom"})
        chart.set_size({"width": 900, "height": 420})
        sheet.insert_chart("J2", chart)
        sheet.freeze_panes(1, 1)
        sheet.set_column(0, 0, 8)
        sheet.set_column(1, len(algo_order), 16)

    workbook.close()


def plot_tradeoff_scatter(
    query_rows: list[dict[str, object]],
    material_rows: list[dict[str, object]],
) -> None:
    query_map = metric_lookup(query_rows, "cross_partition_edge_ratio")
    bytes_map = metric_lookup(material_rows, "bytes_total")

    merged = []
    for key, cross in query_map.items():
        if key not in bytes_map:
            continue
        dataset, algo, k = key
        merged.append(
            {
                "dataset": dataset,
                "algorithm": algo,
                "k": k,
                "cross_partition_edge_ratio": cross,
                "bytes_total": bytes_map[key],
            }
        )

    fig, ax = plt.subplots(figsize=(12, 8))
    for dataset in datasets_for(merged):
        subset = [r for r in merged if r["dataset"] == dataset]
        for algo in algorithms_for(subset):
            pts = [r for r in subset if r["algorithm"] == algo]
            if not pts:
                continue
            xs = [r["cross_partition_edge_ratio"] for r in pts]
            ys = [r["bytes_total"] for r in pts]
            sizes = [70 + 15 * int(r["k"]) for r in pts]
            ax.scatter(
                xs,
                ys,
                s=sizes,
                alpha=0.8,
                color=ALGO_COLORS.get(algo, "#000000"),
                marker=DATASET_MARKERS.get(dataset, "o"),
                edgecolors="black",
                linewidths=0.5,
            )
    ax.set_title("Query Locality vs Materialization Footprint", fontsize=18, fontweight="bold")
    ax.set_xlabel("Cross-partition edge ratio")
    ax.set_ylabel("Materialization footprint (bytes)")
    ax.set_yscale("log")
    ax.grid(True, alpha=0.25)

    algo_handles = [
        Line2D(
            [0],
            [0],
            marker="o",
            linestyle="",
            markerfacecolor=ALGO_COLORS.get(algo, "#000000"),
            markeredgecolor="black",
            label=ALGO_LABELS.get(algo, algo),
            markersize=8,
        )
        for algo in algorithms_for(merged)
    ]
    dataset_handles = [
        Line2D(
            [0],
            [0],
            marker=DATASET_MARKERS.get(dataset, "o"),
            linestyle="",
            color="black",
            label=DATASET_LABELS.get(dataset, dataset),
            markersize=8,
        )
        for dataset in datasets_for(merged)
    ]
    legend1 = ax.legend(handles=algo_handles, title="Algorithm", loc="upper left", bbox_to_anchor=(1.02, 1.0))
    ax.add_artist(legend1)
    ax.legend(handles=dataset_handles, title="Dataset", loc="upper left", bbox_to_anchor=(1.02, 0.42))
    fig.tight_layout()
    fig.savefig(PNG_DIR / "tradeoff_cross_ratio_vs_footprint.png", dpi=180, bbox_inches="tight")
    plt.close(fig)

    workbook = xlsxwriter.Workbook(str(XLSX_DIR / "tradeoff_cross_ratio_vs_footprint.xlsx"))
    fmt_header = workbook.add_format({"bold": True, "bg_color": "#D9E1F2", "border": 1})
    fmt_num = workbook.add_format({"num_format": "0.000000"})
    fmt_bytes = workbook.add_format({"num_format": "0"})
    sheet = workbook.add_worksheet("data")
    sheet.write_row(0, 0, ["dataset", "algorithm", "k", "cross_partition_edge_ratio", "bytes_total"], fmt_header)
    for row_idx, row in enumerate(merged, start=1):
        sheet.write_row(row_idx, 0, [DATASET_LABELS.get(row["dataset"], row["dataset"]), ALGO_LABELS.get(row["algorithm"], row["algorithm"]), row["k"]])
        sheet.write_number(row_idx, 3, row["cross_partition_edge_ratio"], fmt_num)
        sheet.write_number(row_idx, 4, row["bytes_total"], fmt_bytes)
    chart = workbook.add_chart({"type": "scatter", "subtype": "straight_with_markers"})
    for algo in algorithms_for(merged):
        first = None
        last = None
        for idx, row in enumerate(merged, start=1):
            if row["algorithm"] == algo:
                if first is None:
                    first = idx
                last = idx
        if first is None:
            continue
        chart.add_series(
            {
                "name": ALGO_LABELS.get(algo, algo),
                "categories": ["data", first, 3, last, 3],
                "values": ["data", first, 4, last, 4],
                "marker": {"type": "circle", "size": 6, "border": {"color": ALGO_COLORS.get(algo, "#000000")}, "fill": {"color": ALGO_COLORS.get(algo, "#000000")}},
                "line": {"none": True},
            }
        )
    chart.set_title({"name": "Query Locality vs Materialization Footprint"})
    chart.set_x_axis({"name": "Cross-partition edge ratio"})
    chart.set_y_axis({"name": "Materialization footprint (bytes)", "log_base": 10})
    chart.set_legend({"position": "bottom"})
    chart.set_size({"width": 960, "height": 520})
    sheet.insert_chart("G2", chart)
    sheet.set_column(0, 4, 22)
    workbook.close()


def competition_groups(rows: list[dict[str, object]], metric: str) -> dict[tuple[str, int], list[tuple[str, float]]]:
    grouped: dict[tuple[str, int], list[tuple[str, float]]] = defaultdict(list)
    for row in rows:
        value = row.get(metric)
        if isinstance(value, float) and math.isnan(value):
            continue
        grouped[(str(row["dataset"]), int(row["k"]))].append((str(row["algorithm"]), float(value)))
    return grouped


def compute_wins(
    partition_rows: list[dict[str, object]],
    query_rows: list[dict[str, object]],
    material_rows: list[dict[str, object]],
) -> tuple[list[str], dict[str, dict[str, int]]]:
    metric_sources = {
        "cut": partition_rows,
        "balance": partition_rows,
        "time_partition_sec": partition_rows,
        "cross_partition_edge_ratio": query_rows,
        "query_avg_ms": query_rows,
        "bytes_total": material_rows,
        "materialization_time_sec": material_rows,
    }
    winners: dict[str, dict[str, int]] = {metric: defaultdict(int) for metric in metric_sources}
    for metric, rows in metric_sources.items():
        groups = competition_groups(rows, metric)
        for _, series in groups.items():
            if not series:
                continue
            best = min(value for _, value in series)
            for algo, value in series:
                if math.isclose(value, best, rel_tol=1e-12, abs_tol=1e-12):
                    winners[metric][algo] += 1
    algo_order = algorithms_for(
        partition_rows + query_rows + material_rows  # type: ignore[list-item]
    )
    return list(metric_sources.keys()), {metric: {algo: winners[metric].get(algo, 0) for algo in algo_order} for metric in winners}


def plot_wins_by_metric(
    partition_rows: list[dict[str, object]],
    query_rows: list[dict[str, object]],
    material_rows: list[dict[str, object]],
) -> None:
    metrics, wins = compute_wins(partition_rows, query_rows, material_rows)
    algo_order = algorithms_for(partition_rows + query_rows + material_rows)  # type: ignore[list-item]
    x = list(range(len(metrics)))
    width = 0.11
    fig, ax = plt.subplots(figsize=(14, 8))
    for idx, algo in enumerate(algo_order):
        offsets = [v + (idx - (len(algo_order) - 1) / 2) * width for v in x]
        ax.bar(
            offsets,
            [wins[m][algo] for m in metrics],
            width=width,
            label=ALGO_LABELS.get(algo, algo),
            color=ALGO_COLORS.get(algo, "#000000"),
        )
    ax.set_title("Win Count Across Aggregate Metrics", fontsize=18, fontweight="bold")
    ax.set_xticks(x)
    ax.set_xticklabels(
        ["Cut", "Balance", "Partition Time", "Cross Ratio", "Query Avg ms", "Footprint", "Materialize Time"],
        rotation=20,
        ha="right",
    )
    ax.set_ylabel("Winning run groups")
    ax.grid(True, axis="y", alpha=0.25)
    ax.legend(ncol=4, frameon=False)
    fig.tight_layout()
    fig.savefig(PNG_DIR / "wins_by_metric.png", dpi=180, bbox_inches="tight")
    plt.close(fig)

    workbook = xlsxwriter.Workbook(str(XLSX_DIR / "wins_by_metric.xlsx"))
    fmt_header = workbook.add_format({"bold": True, "bg_color": "#D9E1F2", "border": 1})
    sheet = workbook.add_worksheet("wins")
    sheet.write_row(0, 0, ["metric"] + [ALGO_LABELS.get(a, a) for a in algo_order], fmt_header)
    for row_idx, metric in enumerate(metrics, start=1):
        sheet.write(row_idx, 0, metric)
        for col_idx, algo in enumerate(algo_order, start=1):
            sheet.write_number(row_idx, col_idx, wins[metric][algo])
    chart = workbook.add_chart({"type": "column"})
    for col_idx, algo in enumerate(algo_order, start=1):
        chart.add_series(
            {
                "name": ["wins", 0, col_idx],
                "categories": ["wins", 1, 0, len(metrics), 0],
                "values": ["wins", 1, col_idx, len(metrics), col_idx],
                "fill": {"color": ALGO_COLORS.get(algo, "#000000")},
                "border": {"color": ALGO_COLORS.get(algo, "#000000")},
            }
        )
    chart.set_title({"name": "Win Count Across Aggregate Metrics"})
    chart.set_x_axis({"name": "Metric"})
    chart.set_y_axis({"name": "Winning run groups"})
    chart.set_legend({"position": "bottom"})
    chart.set_size({"width": 980, "height": 520})
    sheet.insert_chart("J2", chart)
    sheet.set_column(0, len(algo_order), 16)
    workbook.close()


def average_rank_by_metric(
    rows: list[dict[str, object]],
    metric: str,
    algo_order: list[str],
) -> dict[str, float]:
    grouped = competition_groups(rows, metric)
    ranks: dict[str, list[float]] = defaultdict(list)
    for _, series in grouped.items():
        ordered = sorted(series, key=lambda item: item[1])
        for idx, (algo, _) in enumerate(ordered, start=1):
            ranks[algo].append(float(idx))
    return {algo: (mean(ranks[algo]) if ranks.get(algo) else math.nan) for algo in algo_order}


def plot_average_rank_heatmap(
    partition_rows: list[dict[str, object]],
    query_rows: list[dict[str, object]],
    material_rows: list[dict[str, object]],
) -> None:
    algo_order = algorithms_for(partition_rows + query_rows + material_rows)  # type: ignore[list-item]
    metric_groups = [
        ("cut", partition_rows, "Cut"),
        ("balance", partition_rows, "Balance"),
        ("time_partition_sec", partition_rows, "Partition Time"),
        ("cross_partition_edge_ratio", query_rows, "Cross Ratio"),
        ("query_avg_ms", query_rows, "Query Avg ms"),
        ("bytes_total", material_rows, "Footprint"),
        ("materialization_time_sec", material_rows, "Materialize Time"),
    ]
    matrix = [[average_rank_by_metric(rows, metric, algo_order)[algo] for algo in algo_order] for metric, rows, _ in metric_groups]

    fig, ax = plt.subplots(figsize=(12, 6))
    im = ax.imshow(matrix, aspect="auto", cmap="YlGn_r")
    ax.set_title("Average Rank by Metric", fontsize=18, fontweight="bold")
    ax.set_xticks(range(len(algo_order)))
    ax.set_xticklabels([ALGO_LABELS.get(a, a) for a in algo_order], rotation=30, ha="right")
    ax.set_yticks(range(len(metric_groups)))
    ax.set_yticklabels([label for _, _, label in metric_groups])
    for i, row in enumerate(matrix):
        for j, value in enumerate(row):
            if not math.isnan(value):
                ax.text(j, i, f"{value:.2f}", ha="center", va="center", fontsize=10)
    fig.colorbar(im, ax=ax, fraction=0.03, pad=0.04, label="Average rank (lower is better)")
    fig.tight_layout()
    fig.savefig(PNG_DIR / "average_rank_heatmap.png", dpi=180, bbox_inches="tight")
    plt.close(fig)

    workbook = xlsxwriter.Workbook(str(XLSX_DIR / "average_rank_heatmap.xlsx"))
    fmt_header = workbook.add_format({"bold": True, "bg_color": "#D9E1F2", "border": 1})
    fmt_num = workbook.add_format({"num_format": "0.00"})
    sheet = workbook.add_worksheet("ranks")
    sheet.write_row(0, 0, ["metric"] + [ALGO_LABELS.get(a, a) for a in algo_order], fmt_header)
    for row_idx, (_, _, label) in enumerate(metric_groups, start=1):
        sheet.write(row_idx, 0, label)
        for col_idx, value in enumerate(matrix[row_idx - 1], start=1):
            if not math.isnan(value):
                sheet.write_number(row_idx, col_idx, value, fmt_num)
    sheet.conditional_format(
        1,
        1,
        len(metric_groups),
        len(algo_order),
        {"type": "3_color_scale", "min_color": "#63BE7B", "mid_color": "#FFEB84", "max_color": "#F8696B"},
    )
    sheet.set_column(0, len(algo_order), 18)
    workbook.close()


def write_readme(files: list[str]) -> None:
    text = [
        "Aggregate benchmark charts generated from the final aggregate CSVs.",
        "",
        f"Sources:",
        f"- {PARTITION_CSV.relative_to(ROOT)}",
        f"- {QUERY_CSV.relative_to(ROOT)}",
        f"- {MATERIALIZATION_CSV.relative_to(ROOT)}",
        "",
        "Outputs:",
    ]
    text.extend(f"- {name}" for name in files)
    (OUT_DIR / "README.txt").write_text("\n".join(text) + "\n", encoding="utf-8")


def main() -> int:
    ensure_dirs()
    partition_rows = load_partition_rows()
    query_rows = load_query_rows()
    material_rows = load_materialization_rows()

    plot_dataset_grid(
        rows=partition_rows,
        metric="balance",
        title="Partition Balance Across Datasets",
        ylabel="Balance",
        png_name="partition_balance_vs_k.png",
        workbook_name="partition_balance_vs_k.xlsx",
        description="Line charts per dataset showing partition balance across k for all partitioning algorithms. Lower and closer to 1 is better.",
    )
    plot_dataset_grid(
        rows=partition_rows,
        metric="cut",
        title="Cut Across Datasets",
        ylabel="Cut",
        png_name="partition_cut_vs_k.png",
        workbook_name="partition_cut_vs_k.xlsx",
        description="Line charts per dataset showing cut across k. Lower is better.",
        log_y=True,
    )
    plot_dataset_grid(
        rows=partition_rows,
        metric="bnd",
        title="Boundary Nodes Across Datasets",
        ylabel="Boundary nodes",
        png_name="partition_bnd_vs_k.png",
        workbook_name="partition_bnd_vs_k.xlsx",
        description="Line charts per dataset showing the number of boundary nodes across k. Lower is better.",
        log_y=True,
    )
    plot_dataset_grid(
        rows=partition_rows,
        metric="max_comm_vol",
        title="Max Communication Volume Across Datasets",
        ylabel="Max communication volume",
        png_name="partition_max_comm_vol_vs_k.png",
        workbook_name="partition_max_comm_vol_vs_k.xlsx",
        description="Line charts per dataset showing max communication volume across k. Lower is better.",
        log_y=True,
    )
    plot_dataset_grid(
        rows=partition_rows,
        metric="time_partition_sec",
        title="Partitioning Time Across Datasets",
        ylabel="Partitioning time (sec)",
        png_name="partition_time_vs_k.png",
        workbook_name="partition_time_vs_k.xlsx",
        description="Line charts per dataset showing partitioning runtime across k. Lower is better.",
        log_y=True,
    )
    plot_dataset_grid(
        rows=query_rows,
        metric="cross_partition_edge_ratio",
        title="Cross-partition Edge Ratio Across Query Runs",
        ylabel="Cross-partition edge ratio",
        png_name="query_cross_ratio_vs_k.png",
        workbook_name="query_cross_ratio_vs_k.xlsx",
        description="Line charts per dataset showing query-side cross-partition edge ratio across k. Lower is better.",
    )
    plot_dataset_grid(
        rows=query_rows,
        metric="query_avg_ms",
        title="Average Query Latency Across Query Runs",
        ylabel="Average query latency (ms)",
        png_name="query_avg_ms_vs_k.png",
        workbook_name="query_avg_ms_vs_k.xlsx",
        description="Line charts per dataset showing bounded-load single-instance query average latency across k.",
        log_y=True,
    )
    plot_dataset_grid(
        rows=material_rows,
        metric="bytes_total",
        title="Materialization Footprint Across Datasets",
        ylabel="Materialization footprint (bytes)",
        png_name="materialization_footprint_vs_k.png",
        workbook_name="materialization_footprint_vs_k.xlsx",
        description="Line charts per dataset showing materialized graph footprint across k. Lower is better.",
        log_y=True,
    )
    plot_dataset_grid(
        rows=material_rows,
        metric="materialization_time_sec",
        title="Materialization Time Across Datasets",
        ylabel="Materialization time (sec)",
        png_name="materialization_time_vs_k.png",
        workbook_name="materialization_time_vs_k.xlsx",
        description="Line charts per dataset showing materialization runtime across k. Lower is better.",
        log_y=True,
    )
    plot_tradeoff_scatter(query_rows, material_rows)
    plot_wins_by_metric(partition_rows, query_rows, material_rows)
    plot_average_rank_heatmap(partition_rows, query_rows, material_rows)

    files = sorted(str(p.relative_to(ROOT)) for p in OUT_DIR.rglob("*") if p.is_file())
    write_readme(files)
    print(f"[OK] Wrote charts under {OUT_DIR}")
    print(f"[OK] PNG files: {len(list(PNG_DIR.glob('*.png')))}")
    print(f"[OK] XLSX files: {len(list(XLSX_DIR.glob('*.xlsx')))}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
