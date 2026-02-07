from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


PAIR = "PAXG_XAUT"
STATUS_OPEN_PENDING_CONFIRM = "OPEN_PENDING_CONFIRM"
STATUS_OPEN_CONFIRMED = "OPEN_CONFIRMED"
STATUS_CLOSE_SIGNALLED = "CLOSE_SIGNALLED"
STATUS_CLOSED = "CLOSED"


@dataclass(slots=True)
class MarketSnapshot:
    ts_ms: int
    paxg_bid: float
    paxg_ask: float
    xaut_bid: float
    xaut_ask: float
    spread_open: float
    spread_close: float
    paxg_funding: float | None
    xaut_funding: float | None
    funding_diff_raw: float | None
    funding_diff_annual: float | None
    annual_factor: float
    quote_size_paxg: str
    quote_size_xaut: str
    latency_ms: int


@dataclass(slots=True)
class RuntimeConfig:
    api_url: str
    quote_size: str
    pair: str
    poll_interval_sec: float
    threshold_open: float
    close_buffer: float
    repeat_alert_sec: int
    annual_factor: float
    config_refresh_sec: int
    data_ttl_days: int
    aws_region: str
    ticks_table: str
    positions_table: str
    config_table: str
    alerts_table: str | None
    tg_bot_token: str | None
    tg_allowed_chat_ids: set[str] = field(default_factory=set)


@dataclass(slots=True)
class PositionRecord:
    position_id: str
    status: str
    created_at_ts: int
    updated_at_ts: int
    signal_spread: float
    signal_ts: int
    last_open_alert_ts: int
    entry_spread_actual: float | None = None
    opened_at_confirm_ts: int | None = None
    close_trigger: float | None = None
    close_signalled_ts: int | None = None
    last_close_alert_ts: int | None = None
    close_spread_actual: float | None = None
    closed_at_confirm_ts: int | None = None
    chat_id: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
