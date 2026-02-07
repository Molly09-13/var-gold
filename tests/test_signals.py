import unittest

from src.models import MarketSnapshot, PositionRecord
from src.signals import close_trigger, is_close_signal, is_open_signal, should_repeat


class SignalsTest(unittest.TestCase):
    def _snapshot(self, spread_open: float, spread_close: float) -> MarketSnapshot:
        return MarketSnapshot(
            ts_ms=1,
            paxg_bid=2300.0,
            paxg_ask=2301.0,
            xaut_bid=2299.0,
            xaut_ask=2300.0,
            spread_open=spread_open,
            spread_close=spread_close,
            paxg_funding=0.01,
            xaut_funding=0.005,
            funding_diff_raw=0.005,
            funding_diff_annual=1.825,
            annual_factor=365.0,
            quote_size_paxg="size_100k",
            quote_size_xaut="size_100k",
            latency_ms=10,
        )

    def test_open_signal_threshold(self) -> None:
        snapshot = self._snapshot(spread_open=40.01, spread_close=-39)
        self.assertTrue(is_open_signal(snapshot, 40))
        self.assertFalse(is_open_signal(snapshot, 41))

    def test_close_trigger_rule(self) -> None:
        self.assertEqual(close_trigger(39, 0), -39)
        self.assertEqual(close_trigger(39, 2), -37)

    def test_close_signal_for_position(self) -> None:
        snapshot = self._snapshot(spread_open=42, spread_close=-38.9)
        position = PositionRecord(
            position_id="p1",
            status="OPEN_CONFIRMED",
            created_at_ts=1,
            updated_at_ts=1,
            signal_spread=41,
            signal_ts=1,
            last_open_alert_ts=1,
            entry_spread_actual=39,
            close_trigger=-39,
            metadata={},
        )
        self.assertTrue(is_close_signal(snapshot, position))

    def test_repeat_cooldown(self) -> None:
        now_ms = 1_000_000
        self.assertTrue(should_repeat(None, now_ms, 300))
        self.assertFalse(should_repeat(now_ms - 299_000, now_ms, 300))
        self.assertTrue(should_repeat(now_ms - 300_000, now_ms, 300))


if __name__ == "__main__":
    unittest.main()
