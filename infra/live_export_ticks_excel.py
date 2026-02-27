#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import sys
import time
from datetime import datetime, timezone
from typing import Any

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from infra.export_ticks_excel import write_excel
from src.config_store import load_runtime_config
from src.storage import DynamoStorage


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Live incremental exporter for 24h Excel dashboard data."
    )
    parser.add_argument("--window-hours", type=float, default=24.0, help="Rolling window in hours.")
    parser.add_argument("--interval-sec", type=float, default=20.0, help="Refresh interval seconds.")
    parser.add_argument("--pair", help="Override pair (default from .env).")
    parser.add_argument(
        "--output",
        default=os.path.join(REPO_ROOT, "var_gold_live.xlsx"),
        help="Output xlsx path.",
    )
    return parser.parse_args()


def _sort_rows(rows: list[dict[str, Any]]) -> None:
    rows.sort(key=lambda item: int(item.get("ts_ms", 0)))


def _trim_rows(rows: list[dict[str, Any]], min_ts_ms: int) -> list[dict[str, Any]]:
    return [r for r in rows if int(r.get("ts_ms", 0)) >= min_ts_ms]


def _max_ts_ms(rows: list[dict[str, Any]], default: int) -> int:
    if not rows:
        return default
    return max(int(r.get("ts_ms", 0)) for r in rows)


def _write_atomic(path: str, rows: list[dict[str, Any]]) -> None:
    tmp_path = f"{path}.new"
    write_excel(tmp_path, rows, include_charts=False)
    os.replace(tmp_path, path)


def main() -> None:
    args = parse_args()
    config = load_runtime_config(REPO_ROOT)
    pair = args.pair or config.pair
    output = os.path.abspath(args.output)
    window_ms = int(args.window_hours * 3600 * 1000)
    interval_sec = max(1.0, float(args.interval_sec))

    storage = DynamoStorage(
        region=config.aws_region,
        ticks_table=config.ticks_table,
        positions_table=config.positions_table,
        config_table=config.config_table,
        alerts_table=config.alerts_table,
        data_ttl_days=config.data_ttl_days,
    )

    now_ms = int(time.time() * 1000)
    start_ms = now_ms - window_ms
    rows = storage.query_ticks(pair=pair, start_ms=start_ms, end_ms=now_ms)
    _sort_rows(rows)
    rows = _trim_rows(rows, start_ms)
    last_ts_ms = _max_ts_ms(rows, default=start_ms)
    _write_atomic(output, rows)
    print(
        f"{datetime.now(timezone.utc).isoformat()} init rows={len(rows)} "
        f"window_start={start_ms} last_ts={last_ts_ms}"
    )

    while True:
        loop_start = time.time()
        now_ms = int(time.time() * 1000)
        inc_start_ms = last_ts_ms + 1
        if inc_start_ms <= now_ms:
            new_rows = storage.query_ticks(pair=pair, start_ms=inc_start_ms, end_ms=now_ms)
            if new_rows:
                _sort_rows(new_rows)
                rows.extend(new_rows)
                last_ts_ms = _max_ts_ms(new_rows, default=last_ts_ms)

        min_ts_ms = now_ms - window_ms
        rows = _trim_rows(rows, min_ts_ms)
        _write_atomic(output, rows)

        print(
            f"{datetime.now(timezone.utc).isoformat()} refreshed rows={len(rows)} "
            f"last_ts={last_ts_ms}"
        )
        elapsed = time.time() - loop_start
        delay = interval_sec - elapsed
        if delay > 0:
            time.sleep(delay)


if __name__ == "__main__":
    main()

