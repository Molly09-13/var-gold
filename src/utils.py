from __future__ import annotations

import os
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any


def load_env(env_path: str) -> None:
    """Load environment variables from .env without overriding existing vars."""
    try:
        with open(env_path, "r", encoding="utf-8") as env_file:
            for raw_line in env_file:
                line = raw_line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, value = line.split("=", 1)
                key = key.strip()
                value = value.strip()
                if len(value) >= 2 and value[0] == value[-1] and value[0] in ('"', "'"):
                    value = value[1:-1]
                if key and key not in os.environ:
                    os.environ[key] = value
    except FileNotFoundError:
        return


def now_utc_ms() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def ttl_epoch(days: int) -> int:
    return int(datetime.now(timezone.utc).timestamp()) + max(days, 1) * 86400


def safe_float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def as_decimal(value: float | int | None) -> Decimal | None:
    if value is None:
        return None
    return Decimal(str(value))


def from_decimal(value: Any) -> Any:
    if isinstance(value, Decimal):
        # Keep integer formatting when possible for easier downstream usage.
        if value == value.to_integral_value():
            return int(value)
        return float(value)
    if isinstance(value, dict):
        return {k: from_decimal(v) for k, v in value.items()}
    if isinstance(value, list):
        return [from_decimal(v) for v in value]
    return value
