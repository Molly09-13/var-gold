# var_gold

PAXG/XAUT spread monitor with DynamoDB persistence and Telegram trade workflow.

## Features

- Poll market data every 2 seconds (configurable)
- Compute and store:
  - `spread_open = paxg_bid - xaut_ask`
  - `spread_close = xaut_bid - paxg_ask`
  - `funding_diff_annual = (paxg_funding - xaut_funding) * annual_factor`
- Save tick data into DynamoDB with TTL (default 90 days)
- Telegram bot workflow:
  - open signal alerts
  - `/open` confirmation with actual entry spread
  - close signal alerts based on `spread_close >= -entry_actual + close_buffer`
  - `/close` confirmation with actual close spread
- Dynamic runtime tuning via Telegram `/set` command
- White-list chat authorization
- Optional `TICKS_ONLY_MODE=1` to persist only tick data (no config/position tables)

## Project layout

- `src/main.py`: runtime loop and command routing
- `src/collector.py`: API polling and spread calculations
- `src/storage.py`: DynamoDB persistence
- `src/position_manager.py`: position state machine
- `src/bot.py`: Telegram API integration
- `infra/create_dynamodb_tables.py`: DynamoDB bootstrap script
- `infra/var_gold.service`: systemd service template
- `var_spread_monitor.py`: backward-compatible entrypoint

## Setup

1. Create and activate venv (optional)
2. Install dependencies:

```bash
pip install -r requirements.txt
```

3. Copy env template and fill values:

```bash
cp "env example.TXT" .env
```

4. Create DynamoDB tables:

```bash
python infra/create_dynamodb_tables.py --region ap-northeast-1
```

## Run locally

```bash
python var_spread_monitor.py --log-level INFO
```

## Telegram commands

- `/status`
- `/positions`
- `/open <actual_spread>`
- `/open <signal_id> <actual_spread>`
- `/close <actual_spread>` (only if one open position)
- `/close <signal_id> <actual_spread>`
- `/set open <value>`
- `/set repeat <seconds>`
- `/set annual <factor>`
- `/set close_buffer <value>`
- `/set poll <seconds>`
- `/config`

## systemd (EC2)

- Copy project to `/opt/var_gold`
- Adjust `infra/var_gold.service` user/path if needed
- Install service:

```bash
sudo cp infra/var_gold.service /etc/systemd/system/var_gold.service
sudo systemctl daemon-reload
sudo systemctl enable --now var_gold
sudo systemctl status var_gold
```
