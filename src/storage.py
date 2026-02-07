from __future__ import annotations

import json
import time
import uuid
from decimal import Decimal
from typing import Any, Iterable

try:
    import boto3
    from boto3.dynamodb.conditions import Attr
    from botocore.exceptions import ClientError
except ModuleNotFoundError:  # pragma: no cover - dependency check
    boto3 = None
    Attr = None

    class ClientError(Exception):
        pass

from .models import (
    PAIR,
    PositionRecord,
    STATUS_CLOSE_SIGNALLED,
    STATUS_CLOSED,
    STATUS_OPEN_CONFIRMED,
    STATUS_OPEN_PENDING_CONFIRM,
)
from .utils import from_decimal, ttl_epoch


class DynamoStorage:
    def __init__(
        self,
        region: str,
        ticks_table: str,
        positions_table: str,
        config_table: str,
        alerts_table: str | None,
        data_ttl_days: int,
    ) -> None:
        if boto3 is None:
            raise RuntimeError(
                "boto3 is required. Install dependencies with: pip install -r requirements.txt"
            )
        self.region = region
        self.data_ttl_days = data_ttl_days
        self.ddb = boto3.resource("dynamodb", region_name=region)
        self.ticks_table = self.ddb.Table(ticks_table)
        self.positions_table = self.ddb.Table(positions_table)
        self.config_table = self.ddb.Table(config_table)
        self.alerts_table = self.ddb.Table(alerts_table) if alerts_table else None

    def put_tick(self, item: dict[str, Any]) -> None:
        payload = {
            "pair": item["pair"],
            "ts_ms": Decimal(str(item["ts_ms"])),
            "ttl_epoch": Decimal(str(ttl_epoch(self.data_ttl_days))),
        }
        for key, value in item.items():
            if key in payload:
                continue
            payload[key] = self._to_ddb_value(value)
        self.ticks_table.put_item(Item=payload)

    def create_pending_position(
        self,
        signal_spread: float,
        signal_ts: int,
        metadata: dict[str, Any],
        now_ms: int,
    ) -> PositionRecord:
        position_id = str(uuid.uuid4())
        item = {
            "position_id": position_id,
            "status": STATUS_OPEN_PENDING_CONFIRM,
            "created_at_ts": Decimal(str(now_ms)),
            "updated_at_ts": Decimal(str(now_ms)),
            "signal_spread": Decimal(str(signal_spread)),
            "signal_ts": Decimal(str(signal_ts)),
            "last_open_alert_ts": Decimal(str(now_ms)),
            "metadata_json": json.dumps(metadata, ensure_ascii=True),
        }
        self.positions_table.put_item(Item=item)
        return self._position_from_item(item)

    def get_position(self, position_id: str) -> PositionRecord | None:
        resp = self.positions_table.get_item(Key={"position_id": position_id})
        item = resp.get("Item")
        if not item:
            return None
        return self._position_from_item(item)

    def list_positions(self, statuses: Iterable[str] | None = None) -> list[PositionRecord]:
        items = []
        scan_kwargs: dict[str, Any] = {}
        if statuses:
            values = [s for s in statuses]
            if len(values) == 1:
                scan_kwargs["FilterExpression"] = Attr("status").eq(values[0])
            else:
                expr = Attr("status").eq(values[0])
                for value in values[1:]:
                    expr = expr | Attr("status").eq(value)
                scan_kwargs["FilterExpression"] = expr

        while True:
            resp = self.positions_table.scan(**scan_kwargs)
            items.extend(resp.get("Items", []))
            last_key = resp.get("LastEvaluatedKey")
            if not last_key:
                break
            scan_kwargs["ExclusiveStartKey"] = last_key

        positions = [self._position_from_item(item) for item in items]
        positions.sort(key=lambda p: p.signal_ts)
        return positions

    def confirm_open(
        self,
        position_id: str,
        entry_spread_actual: float,
        close_buffer: float,
        now_ms: int,
        chat_id: str,
    ) -> PositionRecord | None:
        close_trigger = -entry_spread_actual + close_buffer
        try:
            resp = self.positions_table.update_item(
                Key={"position_id": position_id},
                UpdateExpression=(
                    "SET #st=:st, entry_spread_actual=:entry, close_trigger=:close_trigger, "
                    "opened_at_confirm_ts=:opened_at, updated_at_ts=:updated_at, chat_id=:chat_id"
                ),
                ConditionExpression=Attr("status").eq(STATUS_OPEN_PENDING_CONFIRM),
                ExpressionAttributeNames={"#st": "status"},
                ExpressionAttributeValues={
                    ":st": STATUS_OPEN_CONFIRMED,
                    ":entry": Decimal(str(entry_spread_actual)),
                    ":close_trigger": Decimal(str(close_trigger)),
                    ":opened_at": Decimal(str(now_ms)),
                    ":updated_at": Decimal(str(now_ms)),
                    ":chat_id": chat_id,
                },
                ReturnValues="ALL_NEW",
            )
        except ClientError as exc:
            if exc.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException":
                return None
            raise
        return self._position_from_item(resp["Attributes"])

    def mark_open_alert_sent(self, position_id: str, now_ms: int) -> None:
        self.positions_table.update_item(
            Key={"position_id": position_id},
            UpdateExpression="SET last_open_alert_ts=:ts, updated_at_ts=:ts",
            ExpressionAttributeValues={
                ":ts": Decimal(str(now_ms)),
            },
        )

    def mark_close_signalled(self, position_id: str, now_ms: int) -> PositionRecord | None:
        try:
            resp = self.positions_table.update_item(
                Key={"position_id": position_id},
                UpdateExpression=(
                    "SET #st=:st, close_signalled_ts=:ts, last_close_alert_ts=:ts, updated_at_ts=:ts"
                ),
                ConditionExpression=Attr("status").eq(STATUS_OPEN_CONFIRMED),
                ExpressionAttributeNames={"#st": "status"},
                ExpressionAttributeValues={
                    ":st": STATUS_CLOSE_SIGNALLED,
                    ":ts": Decimal(str(now_ms)),
                },
                ReturnValues="ALL_NEW",
            )
            return self._position_from_item(resp["Attributes"])
        except ClientError as exc:
            if exc.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException":
                return None
            raise

    def mark_close_alert_sent(self, position_id: str, now_ms: int) -> PositionRecord | None:
        resp = self.positions_table.update_item(
            Key={"position_id": position_id},
            UpdateExpression="SET last_close_alert_ts=:ts, updated_at_ts=:ts",
            ExpressionAttributeValues={
                ":ts": Decimal(str(now_ms)),
            },
            ReturnValues="ALL_NEW",
        )
        attrs = resp.get("Attributes")
        if not attrs:
            return None
        return self._position_from_item(attrs)

    def close_position(
        self,
        position_id: str,
        close_spread_actual: float,
        now_ms: int,
        chat_id: str,
    ) -> PositionRecord | None:
        try:
            resp = self.positions_table.update_item(
                Key={"position_id": position_id},
                UpdateExpression=(
                    "SET #st=:st, close_spread_actual=:close_actual, closed_at_confirm_ts=:closed_at, "
                    "updated_at_ts=:updated_at, chat_id=:chat_id"
                ),
                ConditionExpression=Attr("status").is_in([STATUS_OPEN_CONFIRMED, STATUS_CLOSE_SIGNALLED]),
                ExpressionAttributeNames={"#st": "status"},
                ExpressionAttributeValues={
                    ":st": STATUS_CLOSED,
                    ":close_actual": Decimal(str(close_spread_actual)),
                    ":closed_at": Decimal(str(now_ms)),
                    ":updated_at": Decimal(str(now_ms)),
                    ":chat_id": chat_id,
                },
                ReturnValues="ALL_NEW",
            )
        except ClientError as exc:
            if exc.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException":
                return None
            raise
        return self._position_from_item(resp["Attributes"])

    def save_config_value(self, key: str, value: Any) -> None:
        item = {
            "config_key": key,
            "updated_at_ts": Decimal(str(int(time.time() * 1000))),
            "value": self._to_ddb_value(value),
        }
        self.config_table.put_item(Item=item)

    def load_config_map(self) -> dict[str, Any]:
        values: dict[str, Any] = {}
        scan_kwargs: dict[str, Any] = {}
        while True:
            resp = self.config_table.scan(**scan_kwargs)
            for item in resp.get("Items", []):
                key = item.get("config_key")
                if not key:
                    continue
                values[str(key)] = from_decimal(item.get("value"))
            last_key = resp.get("LastEvaluatedKey")
            if not last_key:
                break
            scan_kwargs["ExclusiveStartKey"] = last_key
        return values

    def put_alert(self, payload: dict[str, Any]) -> None:
        if not self.alerts_table:
            return
        item = {
            "alert_id": payload.get("alert_id") or str(uuid.uuid4()),
            "ts_ms": Decimal(str(payload["ts_ms"])),
            "alert_type": payload["alert_type"],
            "message": payload["message"],
            "ttl_epoch": Decimal(str(ttl_epoch(self.data_ttl_days))),
        }
        for key, value in payload.items():
            if key in item:
                continue
            item[key] = self._to_ddb_value(value)
        self.alerts_table.put_item(Item=item)

    @staticmethod
    def _to_ddb_value(value: Any) -> Any:
        if isinstance(value, bool) or value is None:
            return value
        if isinstance(value, Decimal):
            return value
        if isinstance(value, int):
            return Decimal(str(value))
        if isinstance(value, float):
            return Decimal(str(value))
        if isinstance(value, list):
            return [DynamoStorage._to_ddb_value(v) for v in value]
        if isinstance(value, dict):
            return {k: DynamoStorage._to_ddb_value(v) for k, v in value.items()}
        return value

    @staticmethod
    def _position_from_item(item: dict[str, Any]) -> PositionRecord:
        data = from_decimal(item)
        metadata_json = data.get("metadata_json")
        metadata: dict[str, Any]
        if metadata_json:
            try:
                metadata = json.loads(str(metadata_json))
            except json.JSONDecodeError:
                metadata = {}
        else:
            metadata = {}

        return PositionRecord(
            position_id=str(data["position_id"]),
            status=str(data["status"]),
            created_at_ts=int(data["created_at_ts"]),
            updated_at_ts=int(data["updated_at_ts"]),
            signal_spread=float(data["signal_spread"]),
            signal_ts=int(data["signal_ts"]),
            last_open_alert_ts=int(data.get("last_open_alert_ts", data["updated_at_ts"])),
            entry_spread_actual=(
                float(data["entry_spread_actual"])
                if data.get("entry_spread_actual") is not None
                else None
            ),
            opened_at_confirm_ts=(
                int(data["opened_at_confirm_ts"])
                if data.get("opened_at_confirm_ts") is not None
                else None
            ),
            close_trigger=(
                float(data["close_trigger"])
                if data.get("close_trigger") is not None
                else None
            ),
            close_signalled_ts=(
                int(data["close_signalled_ts"])
                if data.get("close_signalled_ts") is not None
                else None
            ),
            last_close_alert_ts=(
                int(data["last_close_alert_ts"])
                if data.get("last_close_alert_ts") is not None
                else None
            ),
            close_spread_actual=(
                float(data["close_spread_actual"])
                if data.get("close_spread_actual") is not None
                else None
            ),
            closed_at_confirm_ts=(
                int(data["closed_at_confirm_ts"])
                if data.get("closed_at_confirm_ts") is not None
                else None
            ),
            chat_id=str(data["chat_id"]) if data.get("chat_id") is not None else None,
            metadata=metadata,
        )


def default_pending_metadata(snapshot: dict[str, Any]) -> dict[str, Any]:
    return {
        "pair": snapshot.get("pair", PAIR),
        "spread_open": snapshot.get("spread_open"),
        "spread_close": snapshot.get("spread_close"),
        "funding_diff_annual": snapshot.get("funding_diff_annual"),
    }
