from __future__ import annotations

import os
from copy import deepcopy
from typing import Any

from .models import PAIR, RuntimeConfig
from .utils import load_env


class RuntimeConfigStore:
    DYNAMIC_KEYS = {
        "threshold_open": float,
        "close_buffer": float,
        "repeat_alert_sec": int,
        "annual_factor": float,
        "poll_interval_sec": float,
        "allowed_chat_ids": list,
    }

    def __init__(self, storage, base_config: RuntimeConfig) -> None:
        self.storage = storage
        self._base = base_config
        self._current = deepcopy(base_config)
        self._last_refresh_ms = 0

    def current(self) -> RuntimeConfig:
        return deepcopy(self._current)

    def refresh(self, now_ms: int) -> RuntimeConfig:
        if now_ms - self._last_refresh_ms < self._current.config_refresh_sec * 1000:
            return self.current()
        overrides = self.storage.load_config_map()
        merged = self._merge_overrides(self._base, overrides)
        self._current = merged
        self._last_refresh_ms = now_ms
        return self.current()

    def save_override(self, key: str, value: Any) -> RuntimeConfig:
        self.storage.save_config_value(key, value)
        # Force immediate refresh next cycle.
        self._last_refresh_ms = 0
        return self.current()

    def _merge_overrides(self, base: RuntimeConfig, overrides: dict[str, Any]) -> RuntimeConfig:
        config = deepcopy(base)
        for key, caster in self.DYNAMIC_KEYS.items():
            if key not in overrides:
                continue
            raw = overrides[key]
            try:
                if key == "allowed_chat_ids":
                    parsed = self._parse_chat_ids(raw)
                    config.tg_allowed_chat_ids = parsed
                elif key == "poll_interval_sec":
                    config.poll_interval_sec = max(0.5, float(caster(raw)))
                elif key == "repeat_alert_sec":
                    config.repeat_alert_sec = max(30, int(caster(raw)))
                elif key == "annual_factor":
                    config.annual_factor = float(caster(raw))
                elif key == "threshold_open":
                    config.threshold_open = float(caster(raw))
                elif key == "close_buffer":
                    config.close_buffer = float(caster(raw))
            except (TypeError, ValueError):
                continue
        return config

    @staticmethod
    def _parse_chat_ids(raw: Any) -> set[str]:
        if raw is None:
            return set()
        if isinstance(raw, str):
            parts = [p.strip() for p in raw.split(",") if p.strip()]
            return set(parts)
        if isinstance(raw, list):
            return {str(v).strip() for v in raw if str(v).strip()}
        return {str(raw).strip()} if str(raw).strip() else set()


def load_runtime_config(repo_root: str) -> RuntimeConfig:
    env_path = os.path.join(repo_root, ".env")
    load_env(env_path)

    tg_chat_id = os.getenv("TG_CHAT_ID", "").strip()
    allowed = {v.strip() for v in os.getenv("TG_ALLOWED_CHAT_IDS", "").split(",") if v.strip()}
    if tg_chat_id:
        allowed.add(tg_chat_id)

    return RuntimeConfig(
        api_url=os.getenv(
            "API_URL",
            "https://omni-client-api.prod.ap-northeast-1.variational.io/metadata/stats",
        ),
        quote_size=os.getenv("QUOTE_SIZE", "size_100k"),
        pair=os.getenv("PAIR", PAIR),
        poll_interval_sec=float(os.getenv("POLL_INTERVAL_SEC", "2")),
        threshold_open=float(os.getenv("THRESHOLD_OPEN", "40")),
        close_buffer=float(os.getenv("CLOSE_BUFFER", "0")),
        repeat_alert_sec=int(os.getenv("REPEAT_ALERT_SEC", "300")),
        annual_factor=float(os.getenv("ANNUAL_FACTOR", "365")),
        config_refresh_sec=int(os.getenv("CONFIG_REFRESH_SEC", "30")),
        data_ttl_days=int(os.getenv("DATA_TTL_DAYS", "90")),
        aws_region=os.getenv("AWS_REGION", "ap-northeast-1"),
        ticks_table=os.getenv("DDB_TICKS_TABLE", "var_gold_ticks"),
        positions_table=os.getenv("DDB_POSITIONS_TABLE", "var_gold_positions"),
        config_table=os.getenv("DDB_CONFIG_TABLE", "var_gold_config"),
        alerts_table=os.getenv("DDB_ALERTS_TABLE", "var_gold_alerts") or None,
        tg_bot_token=os.getenv("TG_BOT_TOKEN"),
        tg_allowed_chat_ids=allowed,
    )
