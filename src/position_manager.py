from __future__ import annotations

from datetime import datetime, timezone
from typing import Callable

from .models import (
    MarketSnapshot,
    PositionRecord,
    STATUS_CLOSE_SIGNALLED,
    STATUS_OPEN_CONFIRMED,
    STATUS_OPEN_PENDING_CONFIRM,
)
from .signals import is_close_signal, is_open_signal, should_repeat

Notifier = Callable[[str], None]
NL = chr(10)


class PositionManager:
    def __init__(self, storage) -> None:
        self.storage = storage

    def process_open_signals(self, snapshot: MarketSnapshot, cfg, now_ms: int, notify: Notifier) -> None:
        if not is_open_signal(snapshot, cfg.threshold_open):
            return

        pending = self.storage.list_positions(statuses=[STATUS_OPEN_PENDING_CONFIRM])
        latest_pending = pending[-1] if pending else None

        if latest_pending is None:
            position = self.storage.create_pending_position(
                signal_spread=snapshot.spread_open,
                signal_ts=snapshot.ts_ms,
                metadata={
                    "spread_close": snapshot.spread_close,
                    "funding_diff_annual": snapshot.funding_diff_annual,
                },
                now_ms=now_ms,
            )
            notify(self._open_signal_message(position, snapshot, cfg.threshold_open, is_repeat=False))
            self.storage.put_alert(
                {
                    "ts_ms": now_ms,
                    "alert_type": "OPEN_SIGNAL",
                    "position_id": position.position_id,
                    "message": "open signal created",
                    "spread_open": snapshot.spread_open,
                }
            )
            return

        if should_repeat(latest_pending.last_open_alert_ts, now_ms, cfg.repeat_alert_sec):
            notify(self._open_signal_message(latest_pending, snapshot, cfg.threshold_open, is_repeat=True))
            self.storage.mark_open_alert_sent(latest_pending.position_id, now_ms)
            self.storage.put_alert(
                {
                    "ts_ms": now_ms,
                    "alert_type": "OPEN_SIGNAL_REPEAT",
                    "position_id": latest_pending.position_id,
                    "message": "open signal reminder",
                    "spread_open": snapshot.spread_open,
                }
            )

    def process_close_signals(self, snapshot: MarketSnapshot, cfg, now_ms: int, notify: Notifier) -> None:
        open_positions = self.storage.list_positions(statuses=[STATUS_OPEN_CONFIRMED, STATUS_CLOSE_SIGNALLED])
        for position in open_positions:
            if position.close_trigger is None:
                continue

            if position.status == STATUS_OPEN_CONFIRMED and is_close_signal(snapshot, position):
                updated = self.storage.mark_close_signalled(position.position_id, now_ms)
                if not updated:
                    continue
                notify(self._close_signal_message(updated, snapshot, is_repeat=False))
                self.storage.put_alert(
                    {
                        "ts_ms": now_ms,
                        "alert_type": "CLOSE_SIGNAL",
                        "position_id": position.position_id,
                        "message": "close signal triggered",
                        "spread_close": snapshot.spread_close,
                    }
                )
                continue

            if position.status == STATUS_CLOSE_SIGNALLED and should_repeat(
                position.last_close_alert_ts,
                now_ms,
                cfg.repeat_alert_sec,
            ):
                notify(self._close_signal_message(position, snapshot, is_repeat=True))
                self.storage.mark_close_alert_sent(position.position_id, now_ms)
                self.storage.put_alert(
                    {
                        "ts_ms": now_ms,
                        "alert_type": "CLOSE_SIGNAL_REPEAT",
                        "position_id": position.position_id,
                        "message": "close signal reminder",
                        "spread_close": snapshot.spread_close,
                    }
                )

    def confirm_open(
        self,
        chat_id: str,
        entry_spread_actual: float,
        close_buffer: float,
        now_ms: int,
        position_id: str | None = None,
    ) -> PositionRecord | None:
        target_id = position_id
        if target_id is None:
            pending = self.storage.list_positions(statuses=[STATUS_OPEN_PENDING_CONFIRM])
            if not pending:
                return None
            target_id = pending[-1].position_id
        return self.storage.confirm_open(target_id, entry_spread_actual, close_buffer, now_ms, chat_id)

    def confirm_close(
        self,
        chat_id: str,
        close_spread_actual: float,
        now_ms: int,
        position_id: str | None = None,
    ) -> PositionRecord | None:
        target_id = position_id
        if target_id is None:
            candidates = self.storage.list_positions(statuses=[STATUS_OPEN_CONFIRMED, STATUS_CLOSE_SIGNALLED])
            if len(candidates) != 1:
                return None
            target_id = candidates[0].position_id
        return self.storage.close_position(target_id, close_spread_actual, now_ms, chat_id)

    def list_active_positions(self) -> list[PositionRecord]:
        return self.storage.list_positions(
            statuses=[STATUS_OPEN_PENDING_CONFIRM, STATUS_OPEN_CONFIRMED, STATUS_CLOSE_SIGNALLED]
        )

    @staticmethod
    def _open_signal_message(
        position: PositionRecord,
        snapshot: MarketSnapshot,
        threshold: float,
        is_repeat: bool,
    ) -> str:
        title = "<b>Open Signal Reminder</b>" if is_repeat else "<b>Open Signal</b>"
        lines = [
            title,
            "",
            f"Signal ID: <code>{position.position_id}</code>",
            f"spread_open (PAXG sell - XAUT buy): <b>${snapshot.spread_open:.2f}</b>",
            f"Threshold: ${threshold:.2f}",
            f"spread_close (XAUT sell - PAXG buy): ${snapshot.spread_close:.2f}",
            f"Funding annual diff: {format_optional(snapshot.funding_diff_annual, suffix='%')}",
            "",
            "Confirm open with:",
            f"<code>/open {position.position_id} 39</code>",
            "or if only one pending signal:",
            "<code>/open 39</code>",
        ]
        return NL.join(lines)

    @staticmethod
    def _close_signal_message(position: PositionRecord, snapshot: MarketSnapshot, is_repeat: bool) -> str:
        title = "<b>Close Signal Reminder</b>" if is_repeat else "<b>Close Signal</b>"
        trigger = position.close_trigger if position.close_trigger is not None else 0.0
        lines = [
            title,
            "",
            f"Signal ID: <code>{position.position_id}</code>",
            f"spread_close now: <b>${snapshot.spread_close:.2f}</b>",
            f"close_trigger: ${trigger:.2f}",
            f"entry_actual: {format_optional(position.entry_spread_actual, prefix='$')}",
            "",
            "Confirm close with:",
            f"<code>/close {position.position_id} -38.2</code>",
        ]
        return NL.join(lines)


def format_optional(value: float | None, prefix: str = "", suffix: str = "") -> str:
    if value is None:
        return "N/A"
    return f"{prefix}{value:.4f}{suffix}"


def format_position_summary(position: PositionRecord) -> str:
    updated = datetime.fromtimestamp(position.updated_at_ts / 1000, tz=timezone.utc)
    close_trigger_display = "N/A" if position.close_trigger is None else f"{position.close_trigger:.2f}"
    entry_actual_display = "N/A" if position.entry_spread_actual is None else f"{position.entry_spread_actual:.2f}"
    return (
        f"{position.position_id} | {position.status} | "
        f"entry={entry_actual_display} | close_trigger={close_trigger_display} | "
        f"updated={updated.strftime('%Y-%m-%d %H:%M:%S UTC')}"
    )


def format_status_snapshot(snapshot: MarketSnapshot, threshold_open: float) -> str:
    lines = [
        "<b>Current Spread Status</b>",
        "",
        f"spread_open (PAXG sell - XAUT buy): <b>${snapshot.spread_open:.2f}</b> (threshold ${threshold_open:.2f})",
        f"spread_close (XAUT sell - PAXG buy): ${snapshot.spread_close:.2f}",
        f"funding diff annual: {format_optional(snapshot.funding_diff_annual, suffix='%')}",
        f"quotes: PAXG {snapshot.paxg_bid:.2f}/{snapshot.paxg_ask:.2f} | XAUT {snapshot.xaut_bid:.2f}/{snapshot.xaut_ask:.2f}",
    ]
    return NL.join(lines)
