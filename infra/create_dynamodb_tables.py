#!/usr/bin/env python3
from __future__ import annotations

import argparse

import boto3
from botocore.exceptions import ClientError


def ensure_table(ddb, name: str, key_schema, attr_defs, gsis=None) -> None:
    try:
        ddb.describe_table(TableName=name)
        print(f"Table exists: {name}")
        return
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code")
        if code != "ResourceNotFoundException":
            raise

    kwargs = {
        "TableName": name,
        "KeySchema": key_schema,
        "AttributeDefinitions": attr_defs,
        "BillingMode": "PAY_PER_REQUEST",
    }
    if gsis:
        kwargs["GlobalSecondaryIndexes"] = gsis

    print(f"Creating table: {name}")
    ddb.create_table(**kwargs)
    waiter = ddb.get_waiter("table_exists")
    waiter.wait(TableName=name)
    print(f"Table active: {name}")


def ensure_ttl(ddb, table_name: str, attr_name: str) -> None:
    resp = ddb.describe_time_to_live(TableName=table_name)
    status = resp.get("TimeToLiveDescription", {}).get("TimeToLiveStatus")
    if status in {"ENABLING", "ENABLED"}:
        print(f"TTL already {status}: {table_name}.{attr_name}")
        return
    print(f"Enabling TTL: {table_name}.{attr_name}")
    ddb.update_time_to_live(
        TableName=table_name,
        TimeToLiveSpecification={
            "Enabled": True,
            "AttributeName": attr_name,
        },
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create DynamoDB tables for var_gold")
    parser.add_argument("--region", default="ap-northeast-1")
    parser.add_argument("--ticks-table", default="var_gold_ticks")
    parser.add_argument("--positions-table", default="var_gold_positions")
    parser.add_argument("--config-table", default="var_gold_config")
    parser.add_argument("--alerts-table", default="var_gold_alerts")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    ddb = boto3.client("dynamodb", region_name=args.region)

    ensure_table(
        ddb,
        args.ticks_table,
        key_schema=[
            {"AttributeName": "pair", "KeyType": "HASH"},
            {"AttributeName": "ts_ms", "KeyType": "RANGE"},
        ],
        attr_defs=[
            {"AttributeName": "pair", "AttributeType": "S"},
            {"AttributeName": "ts_ms", "AttributeType": "N"},
        ],
    )

    ensure_table(
        ddb,
        args.positions_table,
        key_schema=[{"AttributeName": "position_id", "KeyType": "HASH"}],
        attr_defs=[
            {"AttributeName": "position_id", "AttributeType": "S"},
            {"AttributeName": "status", "AttributeType": "S"},
            {"AttributeName": "updated_at_ts", "AttributeType": "N"},
        ],
        gsis=[
            {
                "IndexName": "status-updated-index",
                "KeySchema": [
                    {"AttributeName": "status", "KeyType": "HASH"},
                    {"AttributeName": "updated_at_ts", "KeyType": "RANGE"},
                ],
                "Projection": {"ProjectionType": "ALL"},
            }
        ],
    )

    ensure_table(
        ddb,
        args.config_table,
        key_schema=[{"AttributeName": "config_key", "KeyType": "HASH"}],
        attr_defs=[{"AttributeName": "config_key", "AttributeType": "S"}],
    )

    ensure_table(
        ddb,
        args.alerts_table,
        key_schema=[{"AttributeName": "alert_id", "KeyType": "HASH"}],
        attr_defs=[{"AttributeName": "alert_id", "AttributeType": "S"}],
    )

    ensure_ttl(ddb, args.ticks_table, "ttl_epoch")
    ensure_ttl(ddb, args.alerts_table, "ttl_epoch")

    print("All done.")


if __name__ == "__main__":
    main()
