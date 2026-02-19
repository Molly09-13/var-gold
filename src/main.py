from __future__ import annotations

import argparse
import logging
import os
import time
import traceback
from typing import Iterable

from .bot import BotCommand, TelegramBot
from .collector import CollectorError, MarketCollector
from .config_store import RuntimeConfigStore, load_runtime_config
from .models import PAIR
from .position_manager import (
    PositionManager,
    format_position_summary,
    format_status_snapshot,
)
from .storage import DynamoStorage
from .utils import now_utc_ms

LOGGER = logging.getLogger("var_gold")


def _is_truthy_env(value: str | None) -> bool:
    if value is None:
        return False
    return value.strip().lower() in {"1", "true", "yes", "on"}


class MonitorService:
    def __init__(self, repo_root: str) -> None:
        self.repo_root = repo_root
        self.base_config = load_runtime_config(repo_root)
        self.ticks_only_mode = _is_truthy_env(os.getenv("TICKS_ONLY_MODE"))
        self.storage = DynamoStorage(
            region=self.base_config.aws_region,
            ticks_table=self.base_config.ticks_table,
            positions_table=self.base_config.positions_table,
            config_table=self.base_config.config_table,
            alerts_table=self.base_config.alerts_table,
            data_ttl_days=self.base_config.data_ttl_days,
        )
        self.config_store = RuntimeConfigStore(self.storage, self.base_config)
        self.position_manager = PositionManager(self.storage)
        self.last_snapshot = None
        self.last_update_id: int | None = None

        cfg = self.config_store.current()
        self.collector = MarketCollector(
            api_url=cfg.api_url,
            quote_size=cfg.quote_size,
            annual_factor=cfg.annual_factor,
        )
        self.bot = TelegramBot(token=cfg.tg_bot_token, allowed_chat_ids=cfg.tg_allowed_chat_ids)
        if self.ticks_only_mode:
            LOGGER.info("ticks-only mode enabled: skipping config/position DynamoDB tables")

        self.api_failure_count = 0
        self.last_api_failure_alert_ts = 0
        self.api_failure_alert_threshold = int(os.getenv("API_FAILURE_ALERT_THRESHOLD", "3"))
        self.api_failure_alert_cooldown_sec = int(os.getenv("API_FAILURE_ALERT_COOLDOWN", "300"))

    def run(self) -> None:
        LOGGER.info("Service started for pair=%s", PAIR)
        while True:
            loop_start = time.monotonic()
            now_ms = now_utc_ms()

            if self.ticks_only_mode:
                cfg = self.base_config
            else:
                cfg = self.config_store.refresh(now_ms)
            self.collector.annual_factor = cfg.annual_factor
            self.collector.quote_size = cfg.quote_size
            self.bot.update_allowed_chat_ids(cfg.tg_allowed_chat_ids)

            if not self.ticks_only_mode:
                self._poll_bot_commands(cfg, now_ms)

            try:
                snapshot = self.collector.fetch_snapshot()
                self.api_failure_count = 0
            except CollectorError as exc:
                LOGGER.warning("collector failure: %s", exc)
                self._handle_api_failure(str(exc), now_ms)
                self._sleep_remaining(loop_start, cfg.poll_interval_sec)
                continue
            except Exception as exc:
                LOGGER.exception("unexpected collector exception: %s", exc)
                self._handle_api_failure(f"unexpected collector exception: {exc}", now_ms)
                self._sleep_remaining(loop_start, cfg.poll_interval_sec)
                continue

            tick_item = {
                "pair": cfg.pair,
                "ts_ms": snapshot.ts_ms,
                "paxg_bid": snapshot.paxg_bid,
                "paxg_ask": snapshot.paxg_ask,
                "xaut_bid": snapshot.xaut_bid,
                "xaut_ask": snapshot.xaut_ask,
                "spread_open": snapshot.spread_open,
                "spread_close": snapshot.spread_close,
                "paxg_funding": snapshot.paxg_funding,
                "xaut_funding": snapshot.xaut_funding,
                "funding_diff_raw": snapshot.funding_diff_raw,
                "funding_diff_annual": snapshot.funding_diff_annual,
                "annual_factor": snapshot.annual_factor,
                "quote_size_paxg": snapshot.quote_size_paxg,
                "quote_size_xaut": snapshot.quote_size_xaut,
                "latency_ms": snapshot.latency_ms,
                "api_ok": True,
                "api_error": "",
            }
            self.storage.put_tick(tick_item)
            self.last_snapshot = snapshot

            LOGGER.info(
                "spread_open=%.2f spread_close=%.2f funding_annual=%s",
                snapshot.spread_open,
                snapshot.spread_close,
                (
                    "N/A"
                    if snapshot.funding_diff_annual is None
                    else f"{snapshot.funding_diff_annual:.4f}"
                ),
            )

            if not self.ticks_only_mode:
                self.position_manager.process_open_signals(snapshot, cfg, now_ms, self._notify_allowed)
                self.position_manager.process_close_signals(snapshot, cfg, now_ms, self._notify_allowed)

            self._sleep_remaining(loop_start, cfg.poll_interval_sec)

    def _poll_bot_commands(self, cfg, now_ms: int) -> None:
        if not self.bot.enabled:
            return
        try:
            commands, next_offset = self.bot.poll_commands(offset=self.last_update_id, timeout_sec=0)
            self.last_update_id = next_offset
        except Exception as exc:
            LOGGER.warning("telegram getUpdates failed: %s", exc)
            return

        for command in commands:
            self._handle_command(command, cfg, now_ms)

    def _handle_command(self, command: BotCommand, cfg, now_ms: int) -> None:
        chat_id = command.chat_id
        if not self.bot.is_authorized(chat_id):
            self.bot.send_message(chat_id, "Unauthorized chat_id.", parse_mode="HTML")
            return

        parts = command.text.split()
        cmd = parts[0].lower()

        try:
            if cmd in {"/start", "/help"}:
                self.bot.send_message(chat_id, self._help_text())
                return

            if cmd == "/status":
                if self.last_snapshot is None:
                    self.bot.send_message(chat_id, "No market snapshot yet.")
                    return
                message = format_status_snapshot(self.last_snapshot, cfg.threshold_open)
                self.bot.send_message(chat_id, message)
                return

            if cmd == "/positions":
                positions = self.position_manager.list_active_positions()
                if not positions:
                    self.bot.send_message(chat_id, "No active/pending positions.")
                    return
                lines = ["\U0001F4CB <b>Active Positions</b>", ""]
                for pos in positions:
                    lines.append(format_position_summary(pos))
                self.bot.send_message(chat_id, "\n".join(lines))
                return

            if cmd == "/open":
                self._handle_open_command(parts, chat_id, cfg, now_ms)
                return

            if cmd == "/close":
                self._handle_close_command(parts, chat_id, now_ms)
                return

            if cmd == "/set":
                self._handle_set_command(parts, chat_id, now_ms)
                return

            if cmd == "/config":
                self.bot.send_message(chat_id, self._config_text(cfg))
                return

            self.bot.send_message(chat_id, "Unknown command. Use /help.")
        except Exception as exc:
            LOGGER.exception("command handling failed: %s", exc)
            self.bot.send_message(chat_id, f"Command failed: {exc}")

    def _handle_open_command(self, parts: list[str], chat_id: str, cfg, now_ms: int) -> None:
        position_id: str | None
        entry_spread_actual: float

        if len(parts) == 2:
            position_id = None
            entry_spread_actual = float(parts[1])
        elif len(parts) == 3:
            position_id = parts[1]
            entry_spread_actual = float(parts[2])
        else:
            self.bot.send_message(chat_id, "Usage: /open <actual_spread> OR /open <signal_id> <actual_spread>")
            return

        position = self.position_manager.confirm_open(
            chat_id=chat_id,
            entry_spread_actual=entry_spread_actual,
            close_buffer=cfg.close_buffer,
            now_ms=now_ms,
            position_id=position_id,
        )

        if not position:
            self.bot.send_message(chat_id, "No pending signal matched (or already confirmed).")
            return

        self.bot.send_message(
            chat_id,
            (
                "\u2705 <b>Open Confirmed</b>\n\n"
                f"Position ID: <code>{position.position_id}</code>\n"
                f"entry_actual: ${position.entry_spread_actual:.2f}\n"
                f"close_trigger: ${position.close_trigger:.2f}"
            ),
        )
        self.storage.put_alert(
            {
                "ts_ms": now_ms,
                "alert_type": "OPEN_CONFIRMED",
                "position_id": position.position_id,
                "message": "position open confirmed",
                "entry_spread_actual": entry_spread_actual,
            }
        )

    def _handle_close_command(self, parts: list[str], chat_id: str, now_ms: int) -> None:
        position_id: str | None = None
        close_spread_actual: float

        if len(parts) == 2:
            position_id = None
            close_spread_actual = float(parts[1])
        elif len(parts) == 3:
            position_id = parts[1]
            close_spread_actual = float(parts[2])
        else:
            self.bot.send_message(chat_id, "Usage: /close <actual_spread> OR /close <signal_id> <actual_spread>")
            return

        position = self.position_manager.confirm_close(
            chat_id=chat_id,
            close_spread_actual=close_spread_actual,
            now_ms=now_ms,
            position_id=position_id,
        )

        if not position:
            self.bot.send_message(chat_id, "No open position matched. Provide signal_id if multiple are open.")
            return

        self.bot.send_message(
            chat_id,
            (
                "\u2705 <b>Close Confirmed</b>\n\n"
                f"Position ID: <code>{position.position_id}</code>\n"
                f"close_actual: ${position.close_spread_actual:.2f}"
            ),
        )
        self.storage.put_alert(
            {
                "ts_ms": now_ms,
                "alert_type": "CLOSE_CONFIRMED",
                "position_id": position.position_id,
                "message": "position close confirmed",
                "close_spread_actual": close_spread_actual,
            }
        )

    def _handle_set_command(self, parts: list[str], chat_id: str, now_ms: int) -> None:
        if len(parts) != 3:
            self.bot.send_message(chat_id, "Usage: /set <open|repeat|annual|close_buffer|poll> <value>")
            return

        key_alias = parts[1].strip().lower()
        raw_value = parts[2].strip()

        key_map = {
            "open": ("threshold_open", float),
            "repeat": ("repeat_alert_sec", int),
            "annual": ("annual_factor", float),
            "close_buffer": ("close_buffer", float),
            "poll": ("poll_interval_sec", float),
        }

        if key_alias not in key_map:
            self.bot.send_message(chat_id, "Unsupported key. Allowed: open, repeat, annual, close_buffer, poll")
            return

        key, caster = key_map[key_alias]
        try:
            value = caster(raw_value)
        except ValueError:
            self.bot.send_message(chat_id, "Invalid value type.")
            return

        self.config_store.save_override(key, value)
        updated_cfg = self.config_store.refresh(now_ms)
        effective_map = {
            "threshold_open": updated_cfg.threshold_open,
            "repeat_alert_sec": updated_cfg.repeat_alert_sec,
            "annual_factor": updated_cfg.annual_factor,
            "close_buffer": updated_cfg.close_buffer,
            "poll_interval_sec": updated_cfg.poll_interval_sec,
        }
        self.bot.send_message(chat_id, f"Updated {key} to {effective_map[key]}")

    def _handle_api_failure(self, error: str, now_ms: int) -> None:
        self.api_failure_count += 1
        if self.api_failure_count < self.api_failure_alert_threshold:
            return
        if now_ms - self.last_api_failure_alert_ts < self.api_failure_alert_cooldown_sec * 1000:
            return

        self.last_api_failure_alert_ts = now_ms
        self._notify_allowed(
            "\u26A0\ufe0f <b>API Failure Alert</b>\n\n"
            f"consecutive_failures: {self.api_failure_count}\n"
            f"reason: {error}"
        )

    def _notify_allowed(self, message: str) -> None:
        if self.bot.enabled and self.bot.allowed_chat_ids:
            for chat_id in self.bot.allowed_chat_ids:
                try:
                    self.bot.send_message(chat_id, message)
                except Exception as exc:
                    LOGGER.warning("telegram send failed chat_id=%s err=%s", chat_id, exc)
        else:
            LOGGER.info("notification skipped (bot disabled): %s", message)

    @staticmethod
    def _sleep_remaining(loop_start: float, interval_sec: float) -> None:
        elapsed = time.monotonic() - loop_start
        delay = interval_sec - elapsed
        if delay > 0:
            time.sleep(delay)

    @staticmethod
    def _help_text() -> str:
        return (
            "\U0001F916 <b>var_gold bot commands</b>\n\n"
            "/status - latest spread snapshot\n"
            "/positions - list active/pending positions\n"
            "/open 39 - confirm latest pending signal\n"
            "/open <signal_id> 39 - confirm specific pending signal\n"
            "/close -38.2 - close when only one open position\n"
            "/close <signal_id> -38.2 - close specific position\n"
            "/set open 40 - set open threshold\n"
            "/set repeat 300 - set repeat alert seconds\n"
            "/set annual 365 - set annual factor\n"
            "/set close_buffer 0 - set close safety buffer\n"
            "/set poll 2 - set polling interval seconds\n"
            "/config - show current runtime config"
        )

    @staticmethod
    def _config_text(cfg) -> str:
        chats = ", ".join(sorted(cfg.tg_allowed_chat_ids)) if cfg.tg_allowed_chat_ids else "(none)"
        return (
            "\u2699\ufe0f <b>Runtime Config</b>\n\n"
            f"threshold_open: {cfg.threshold_open}\n"
            f"close_buffer: {cfg.close_buffer}\n"
            f"repeat_alert_sec: {cfg.repeat_alert_sec}\n"
            f"annual_factor: {cfg.annual_factor}\n"
            f"poll_interval_sec: {cfg.poll_interval_sec}\n"
            f"allowed_chat_ids: {chats}"
        )


def configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )


def parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="PAXG/XAUT monitoring service")
    parser.add_argument("--log-level", default=os.getenv("LOG_LEVEL", "INFO"))
    parser.add_argument(
        "--repo-root",
        default=os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        help="Project root containing .env",
    )
    return parser.parse_args(list(argv) if argv else None)


def run(argv: Iterable[str] | None = None) -> None:
    args = parse_args(argv)
    configure_logging(args.log_level)
    service = MonitorService(repo_root=args.repo_root)
    service.run()


def main() -> None:
    try:
        run()
    except KeyboardInterrupt:
        LOGGER.info("service stopped by keyboard interrupt")
    except Exception as exc:
        LOGGER.error("service crashed: %s", exc)
        LOGGER.error(traceback.format_exc())
        raise


if __name__ == "__main__":
    main()
