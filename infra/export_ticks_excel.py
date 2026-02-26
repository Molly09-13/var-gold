#!/usr/bin/env python3
from __future__ import annotations

import argparse
import math
import os
import statistics
import sys
from datetime import datetime, timedelta, timezone
from typing import Any

import xlsxwriter

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from src.config_store import load_runtime_config
from src.storage import DynamoStorage


def parse_iso_datetime(raw: str) -> datetime:
    value = raw.strip()
    if value.isdigit():
        ts = int(value)
        if len(value) >= 13:
            return datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
        return datetime.fromtimestamp(ts, tz=timezone.utc)

    if value.endswith("Z"):
        value = value[:-1] + "+00:00"

    try:
        dt = datetime.fromisoformat(value)
    except ValueError as exc:  # pragma: no cover - user input validation
        raise argparse.ArgumentTypeError(f"Invalid datetime: {raw}") from exc

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def resolve_window(args: argparse.Namespace) -> tuple[datetime, datetime]:
    if args.date:
        day = datetime.strptime(args.date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        start_dt = day
        end_dt = day + timedelta(days=1) - timedelta(milliseconds=1)
        return start_dt, end_dt

    if args.start and args.end:
        start_dt = parse_iso_datetime(args.start)
        end_dt = parse_iso_datetime(args.end)
        return start_dt, end_dt

    today = datetime.now(timezone.utc).date()
    day = datetime.combine(today - timedelta(days=1), datetime.min.time(), tzinfo=timezone.utc)
    start_dt = day
    end_dt = day + timedelta(days=1) - timedelta(milliseconds=1)
    return start_dt, end_dt


def build_output_path(args: argparse.Namespace, start_dt: datetime, end_dt: datetime) -> str:
    if args.output:
        return args.output

    if args.date:
        suffix = args.date
    else:
        suffix = f"{start_dt.strftime('%Y%m%dT%H%M%S')}_{end_dt.strftime('%Y%m%dT%H%M%S')}"

    filename = f"var_gold_ticks_{suffix}.xlsx"
    return os.path.join(REPO_ROOT, filename)


def write_excel(path: str, rows: list[dict[str, Any]]) -> None:
    workbook = xlsxwriter.Workbook(path, {"remove_timezone": True})
    data_ws = workbook.add_worksheet("data")
    chart_ws = workbook.add_worksheet("charts")

    header_fmt = workbook.add_format({"bold": True, "bg_color": "#F0F0F0"})
    time_fmt = workbook.add_format({"num_format": "yyyy-mm-dd hh:mm:ss"})

    columns = [
        ("timestamp_utc", "ts_utc"),
        ("ts_ms", "ts_ms"),
        ("open_bps", "open_bps"),
        ("close_bps", "close_bps"),
        ("spread_open", "spread_open"),
        ("spread_close", "spread_close"),
        ("paxg_funding", "paxg_funding"),
        ("xaut_funding", "xaut_funding"),
        ("funding_diff_raw", "funding_diff_raw"),
        ("funding_diff_annual", "funding_diff_annual"),
        ("paxg_bid", "paxg_bid"),
        ("paxg_ask", "paxg_ask"),
        ("xaut_bid", "xaut_bid"),
        ("xaut_ask", "xaut_ask"),
        ("latency_ms", "latency_ms"),
        ("annual_factor", "annual_factor"),
        ("quote_size_paxg", "quote_size_paxg"),
        ("quote_size_xaut", "quote_size_xaut"),
    ]

    for col_idx, (header, _) in enumerate(columns):
        data_ws.write(0, col_idx, header, header_fmt)

    data_ws.freeze_panes(1, 0)
    data_ws.set_column(0, 0, 20)
    data_ws.set_column(1, 7, 16)
    data_ws.set_column(8, len(columns) - 1, 14)

    excel_epoch = datetime(1899, 12, 30)

    def get_value(item: dict[str, Any], key: str) -> Any:
        if key == "open_bps":
            paxg_sell = item.get("paxg_bid")
            xaut_buy = item.get("xaut_ask")
            if paxg_sell is None or xaut_buy is None:
                return None
            denom = float(paxg_sell)
            if denom == 0:
                return None
            return (float(paxg_sell) - float(xaut_buy)) / denom * 10000
        if key == "close_bps":
            paxg_sell = item.get("paxg_bid")
            xaut_sell = item.get("xaut_bid")
            paxg_buy = item.get("paxg_ask")
            if paxg_sell is None or xaut_sell is None or paxg_buy is None:
                return None
            denom = float(paxg_sell)
            if denom == 0:
                return None
            return (float(xaut_sell) - float(paxg_buy)) / denom * 10000
        return item.get(key)

    def percentile(values: list[float], p: float) -> float | None:
        if not values:
            return None
        if len(values) == 1:
            return values[0]
        sorted_vals = sorted(values)
        k = (len(sorted_vals) - 1) * p
        f = math.floor(k)
        c = math.ceil(k)
        if f == c:
            return sorted_vals[int(k)]
        return sorted_vals[f] + (sorted_vals[c] - sorted_vals[f]) * (k - f)

    stats_keys = ["open_bps", "close_bps"]
    stats_values: dict[str, list[float]] = {key: [] for key in stats_keys}

    for row_idx, item in enumerate(rows, start=1):
        ts_ms = item.get("ts_ms")
        if ts_ms is not None:
            dt = datetime.fromtimestamp(int(ts_ms) / 1000, tz=timezone.utc).replace(tzinfo=None)
            excel_dt = (dt - excel_epoch).total_seconds() / 86400
            data_ws.write_number(row_idx, 0, excel_dt, time_fmt)
        else:
            data_ws.write_blank(row_idx, 0, None)

        values = []
        for _, key in columns[1:]:
            values.append(get_value(item, key))

        for col_offset, value in enumerate(values, start=1):
            col_idx = col_offset
            if value is None:
                data_ws.write_blank(row_idx, col_idx, None)
            elif isinstance(value, bool):
                data_ws.write_boolean(row_idx, col_idx, value)
            elif isinstance(value, (int, float)):
                data_ws.write_number(row_idx, col_idx, float(value))
            else:
                data_ws.write_string(row_idx, col_idx, str(value))

        for key in stats_keys:
            val = get_value(item, key)
            if isinstance(val, (int, float)) and not math.isnan(float(val)):
                stats_values[key].append(float(val))

    last_row = len(rows)
    if last_row > 0:
        col_map = {key: idx for idx, (_, key) in enumerate(columns)}
        time_col = col_map["ts_utc"]

        axis_common = {
            "name": "UTC time",
            "date_axis": True,
            "num_format": "mm-dd hh:mm",
            "major_gridlines": {"visible": True, "line": {"color": "#E5E7EB"}},
        }

        def add_series(chart, key: str, color: str) -> None:
            chart.add_series(
                {
                    "name": key,
                    "categories": ["data", 1, time_col, last_row, time_col],
                    "values": ["data", 1, col_map[key], last_row, col_map[key]],
                    "line": {"color": color, "width": 1.25},
                    "marker": {"type": "none"},
                }
            )

        spread_chart = workbook.add_chart({"type": "scatter", "subtype": "straight"})
        spread_chart.set_title({"name": "Spread (bps)"})
        spread_chart.set_x_axis(axis_common)
        spread_chart.set_y_axis(
            {"name": "bps", "major_gridlines": {"visible": True, "line": {"color": "#E5E7EB"}}}
        )
        spread_chart.set_legend({"position": "bottom"})
        spread_chart.set_plotarea({"border": {"none": True}})

        add_series(spread_chart, "open_bps", "#3B82F6")
        add_series(spread_chart, "close_bps", "#EF4444")

        funding_chart = workbook.add_chart({"type": "scatter", "subtype": "straight"})
        funding_chart.set_title({"name": "Funding"})
        funding_chart.set_x_axis(axis_common)
        funding_chart.set_y_axis(
            {"name": "Funding", "major_gridlines": {"visible": True, "line": {"color": "#E5E7EB"}}}
        )
        funding_chart.set_legend({"position": "bottom"})
        funding_chart.set_plotarea({"border": {"none": True}})

        add_series(funding_chart, "paxg_funding", "#F59E0B")
        add_series(funding_chart, "xaut_funding", "#8B5CF6")
        add_series(funding_chart, "funding_diff_raw", "#10B981")

        chart_ws.insert_chart("A1", spread_chart, {"x_scale": 1.6, "y_scale": 1.25})
        chart_ws.insert_chart("A22", funding_chart, {"x_scale": 1.6, "y_scale": 1.1})

        stats_header_fmt = workbook.add_format({"bold": True, "bg_color": "#F3F4F6"})
        stats_num_fmt = workbook.add_format({"num_format": "0.00"})

        stats_start_row = 0
        stats_start_col = 12  # column M
        headers = ["series", "mean", "median", "stddev", "p95"]
        for offset, title in enumerate(headers):
            chart_ws.write(stats_start_row, stats_start_col + offset, title, stats_header_fmt)

        for idx, key in enumerate(stats_keys, start=1):
            values = stats_values.get(key, [])
            if values:
                mean_val = statistics.mean(values)
                median_val = statistics.median(values)
                std_val = statistics.pstdev(values)
                p95_val = percentile(values, 0.95)
            else:
                mean_val = median_val = std_val = p95_val = None

            row = stats_start_row + idx
            chart_ws.write_string(row, stats_start_col, key)
            for col_offset, value in enumerate([mean_val, median_val, std_val, p95_val], start=1):
                if value is None:
                    chart_ws.write_blank(row, stats_start_col + col_offset, None)
                else:
                    chart_ws.write_number(row, stats_start_col + col_offset, value, stats_num_fmt)

        chart_ws.set_column(stats_start_col, stats_start_col + 4, 14)
    else:
        chart_ws.write("A1", "No data for selected window.")

    workbook.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export tick data to Excel with charts.")
    parser.add_argument("--date", help="UTC date (YYYY-MM-DD).")
    parser.add_argument("--start", help="Start datetime (ISO 8601 or epoch seconds/ms).")
    parser.add_argument("--end", help="End datetime (ISO 8601 or epoch seconds/ms).")
    parser.add_argument("--pair", help="Override pair (default: from .env).")
    parser.add_argument("--output", help="Output xlsx path.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    start_dt, end_dt = resolve_window(args)
    if end_dt < start_dt:
        raise SystemExit("End datetime must be >= start datetime.")

    start_ms = int(start_dt.timestamp() * 1000)
    end_ms = int(end_dt.timestamp() * 1000)

    config = load_runtime_config(REPO_ROOT)
    pair = args.pair or config.pair

    storage = DynamoStorage(
        region=config.aws_region,
        ticks_table=config.ticks_table,
        positions_table=config.positions_table,
        config_table=config.config_table,
        alerts_table=config.alerts_table,
        data_ttl_days=config.data_ttl_days,
    )

    rows = storage.query_ticks(pair=pair, start_ms=start_ms, end_ms=end_ms)
    rows.sort(key=lambda item: item.get("ts_ms", 0))

    output_path = build_output_path(args, start_dt, end_dt)
    write_excel(output_path, rows)

    print(
        "Exported %d rows for %s (%s to %s) -> %s"
        % (len(rows), pair, start_dt.isoformat(), end_dt.isoformat(), output_path)
    )


if __name__ == "__main__":
    main()
