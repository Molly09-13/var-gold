from __future__ import annotations

from .models import MarketSnapshot, PositionRecord


def is_open_signal(snapshot: MarketSnapshot, threshold_open: float) -> bool:
    return snapshot.spread_open >= threshold_open


def close_trigger(entry_spread_actual: float, close_buffer: float) -> float:
    return -entry_spread_actual + close_buffer


def is_close_signal(snapshot: MarketSnapshot, position: PositionRecord) -> bool:
    if position.close_trigger is None:
        return False
    return snapshot.spread_close >= position.close_trigger


def should_repeat(last_alert_ts: int | None, now_ms: int, repeat_alert_sec: int) -> bool:
    if last_alert_ts is None:
        return True
    return now_ms - last_alert_ts >= repeat_alert_sec * 1000
