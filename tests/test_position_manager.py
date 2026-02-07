import types
import unittest

from src.models import (
    MarketSnapshot,
    PositionRecord,
    STATUS_CLOSE_SIGNALLED,
    STATUS_OPEN_CONFIRMED,
    STATUS_OPEN_PENDING_CONFIRM,
)
from src.position_manager import PositionManager


class FakeStorage:
    def __init__(self) -> None:
        self.positions = []
        self.alerts = []

    def list_positions(self, statuses=None):
        if not statuses:
            return list(self.positions)
        return [p for p in self.positions if p.status in statuses]

    def create_pending_position(self, signal_spread, signal_ts, metadata, now_ms):
        position = PositionRecord(
            position_id="pending-1",
            status=STATUS_OPEN_PENDING_CONFIRM,
            created_at_ts=now_ms,
            updated_at_ts=now_ms,
            signal_spread=signal_spread,
            signal_ts=signal_ts,
            last_open_alert_ts=now_ms,
            metadata=metadata,
        )
        self.positions.append(position)
        return position

    def put_alert(self, payload):
        self.alerts.append(payload)

    def mark_open_alert_sent(self, position_id, now_ms):
        for p in self.positions:
            if p.position_id == position_id:
                p.last_open_alert_ts = now_ms
                p.updated_at_ts = now_ms

    def mark_close_signalled(self, position_id, now_ms):
        for p in self.positions:
            if p.position_id == position_id and p.status == STATUS_OPEN_CONFIRMED:
                p.status = STATUS_CLOSE_SIGNALLED
                p.close_signalled_ts = now_ms
                p.last_close_alert_ts = now_ms
                p.updated_at_ts = now_ms
                return p
        return None

    def mark_close_alert_sent(self, position_id, now_ms):
        for p in self.positions:
            if p.position_id == position_id:
                p.last_close_alert_ts = now_ms
                p.updated_at_ts = now_ms
                return p
        return None


class PositionManagerTest(unittest.TestCase):
    def _snapshot(self, spread_open=41.0, spread_close=-38.5):
        return MarketSnapshot(
            ts_ms=1000,
            paxg_bid=2300,
            paxg_ask=2301,
            xaut_bid=2299,
            xaut_ask=2300,
            spread_open=spread_open,
            spread_close=spread_close,
            paxg_funding=0.01,
            xaut_funding=0.005,
            funding_diff_raw=0.005,
            funding_diff_annual=1.825,
            annual_factor=365,
            quote_size_paxg="size_100k",
            quote_size_xaut="size_100k",
            latency_ms=20,
        )

    def test_create_pending_open_signal(self):
        storage = FakeStorage()
        manager = PositionManager(storage)
        cfg = types.SimpleNamespace(threshold_open=40.0, repeat_alert_sec=300)
        sent = []

        manager.process_open_signals(self._snapshot(spread_open=42.0), cfg, now_ms=2_000_000, notify=sent.append)

        self.assertEqual(len(storage.positions), 1)
        self.assertEqual(storage.positions[0].status, STATUS_OPEN_PENDING_CONFIRM)
        self.assertEqual(len(sent), 1)

    def test_close_signal_promotes_position(self):
        storage = FakeStorage()
        storage.positions.append(
            PositionRecord(
                position_id="pos-1",
                status=STATUS_OPEN_CONFIRMED,
                created_at_ts=1,
                updated_at_ts=1,
                signal_spread=42,
                signal_ts=1,
                last_open_alert_ts=1,
                entry_spread_actual=39,
                close_trigger=-39,
                metadata={},
            )
        )
        manager = PositionManager(storage)
        cfg = types.SimpleNamespace(threshold_open=40.0, repeat_alert_sec=300)
        sent = []

        manager.process_close_signals(self._snapshot(spread_close=-38.9), cfg, now_ms=2_500_000, notify=sent.append)

        self.assertEqual(storage.positions[0].status, STATUS_CLOSE_SIGNALLED)
        self.assertEqual(len(sent), 1)


if __name__ == "__main__":
    unittest.main()
