#!/usr/bin/env python3
"""
Variational PAXG/XAUT 价差监控脚本
监测两个黄金稳定币的套利价差和资金费率
"""

import os
import requests
import time
import traceback
from datetime import datetime

def load_env(env_path: str):
    """从 .env 加载环境变量（不覆盖已有环境变量）"""
    try:
        with open(env_path, "r", encoding="utf-8") as env_file:
            for raw_line in env_file:
                line = raw_line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, value = line.split("=", 1)
                key = key.strip()
                value = value.strip()
                if (
                    len(value) >= 2
                    and value[0] == value[-1]
                    and value[0] in ("\"", "'")
                ):
                    value = value[1:-1]
                if key and key not in os.environ:
                    os.environ[key] = value
    except FileNotFoundError:
        return


ENV_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
load_env(ENV_PATH)

# Telegram 配置（来自环境变量/.env）
TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN")
TG_CHAT_ID = os.getenv("TG_CHAT_ID")

# 监控配置
API_URL = "https://omni-client-api.prod.ap-northeast-1.variational.io/metadata/stats"
SPREAD_THRESHOLD = 30  # 价差阈值
CHECK_INTERVAL = 30    # 检查间隔（秒）
QUOTE_SIZE = "size_100k"  # 使用10万美元报价

# 避免重复通知
last_alert_time = {"short_paxg": 0.0, "short_xaut": 0.0}
ALERT_COOLDOWN = 300  # 5分钟内不重复通知

# API 异常通知
API_FAILURE_ALERT_THRESHOLD = 3
API_FAILURE_ALERT_COOLDOWN = 300
api_failure_count = 0
last_api_failure_alert_time = 0.0


def send_telegram(message: str):
    """发送 Telegram 通知"""
    if not TG_BOT_TOKEN or not TG_CHAT_ID:
        print("TG配置缺失，跳过通知")
        return
    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
    data = {"chat_id": TG_CHAT_ID, "text": message, "parse_mode": "HTML"}
    try:
        requests.post(url, data=data, timeout=10)
    except Exception as e:
        print(f"TG发送失败: {e}")


def fetch_data() -> tuple[dict | None, str | None]:
    """获取 Variational API 数据"""
    try:
        resp = requests.get(API_URL, timeout=10)
        resp.raise_for_status()
        return resp.json(), None
    except Exception as e:
        return None, str(e)


def safe_float(value) -> float | None:
    """将值安全转换为 float"""
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def handle_api_anomaly(reason: str, error: str | None = None):
    """记录并按阈值/冷却发送 API 异常通知"""
    global api_failure_count, last_api_failure_alert_time
    api_failure_count += 1
    current_time = time.monotonic()
    if (
        api_failure_count >= API_FAILURE_ALERT_THRESHOLD
        and current_time - last_api_failure_alert_time > API_FAILURE_ALERT_COOLDOWN
    ):
        details = f"{reason}"
        if error:
            details += f" | {error}"
        send_telegram(
            "⚠️ <b>API异常</b>\n\n"
            f"连续失败次数: {api_failure_count}\n"
            f"原因: {details}"
        )
        last_api_failure_alert_time = current_time


def get_coin_data(listings: list, ticker: str) -> dict | None:
    """从 listings 中获取指定币种数据"""
    for item in listings:
        if not isinstance(item, dict):
            continue
        if item.get("ticker") == ticker:
            return item
    return None


def get_quote(item: dict, quote_size: str) -> tuple[float, float, str] | None:
    """获取报价，优先指定档位，缺失时回退到可用档位"""
    quotes = item.get("quotes")
    if not isinstance(quotes, dict) or not quotes:
        return None

    def extract(quote: dict) -> tuple[float, float] | None:
        if not isinstance(quote, dict):
            return None
        bid = safe_float(quote.get("bid"))
        ask = safe_float(quote.get("ask"))
        if bid is None or ask is None:
            return None
        return bid, ask

    if quote_size in quotes:
        preferred = extract(quotes.get(quote_size))
        if preferred:
            return preferred[0], preferred[1], quote_size

    for key in sorted(quotes.keys(), key=lambda k: str(k)):
        fallback = extract(quotes.get(key))
        if fallback:
            return fallback[0], fallback[1], key

    return None


def main():
    global last_alert_time, api_failure_count, last_api_failure_alert_time
    print(f"开始监控 PAXG/XAUT 价差 (阈值: ${SPREAD_THRESHOLD})")
    print(f"使用报价规模: {QUOTE_SIZE}")
    print("-" * 60)

    while True:
        data, error = fetch_data()
        if not data:
            if error:
                print(f"API请求失败: {error}")
            handle_api_anomaly("API请求失败", error)
            time.sleep(CHECK_INTERVAL)
            continue
        if not isinstance(data, dict):
            print("API返回数据结构异常")
            handle_api_anomaly("API返回数据结构异常")
            time.sleep(CHECK_INTERVAL)
            continue

        listings = data.get("listings")
        if not isinstance(listings, list):
            print("API返回 listings 缺失或非列表")
            handle_api_anomaly("API返回 listings 缺失或非列表")
            time.sleep(CHECK_INTERVAL)
            continue

        paxg = get_coin_data(listings, "PAXG")
        xaut = get_coin_data(listings, "XAUT")

        if not paxg or not xaut:
            print("未找到 PAXG 或 XAUT 数据")
            handle_api_anomaly("未找到 PAXG 或 XAUT 数据")
            time.sleep(CHECK_INTERVAL)
            continue

        paxg_quote = get_quote(paxg, QUOTE_SIZE)
        xaut_quote = get_quote(xaut, QUOTE_SIZE)

        if not paxg_quote or not xaut_quote:
            print("报价数据缺失")
            handle_api_anomaly("报价数据缺失")
            time.sleep(CHECK_INTERVAL)
            continue
        api_failure_count = 0

        paxg_bid, paxg_ask, paxg_quote_size = paxg_quote
        xaut_bid, xaut_ask, xaut_quote_size = xaut_quote

        # 资金费率
        paxg_fr = safe_float(paxg.get("funding_rate"))
        xaut_fr = safe_float(xaut.get("funding_rate"))
        paxg_fr_display = f"{paxg_fr:.4f}%" if paxg_fr is not None else "N/A"
        xaut_fr_display = f"{xaut_fr:.4f}%" if xaut_fr is not None else "N/A"

        # 计算套利价差
        # 做空 PAXG + 做多 XAUT: 以 PAXG bid 卖出，以 XAUT ask 买入
        spread_short_paxg = paxg_bid - xaut_ask
        # 做空 XAUT + 做多 PAXG: 以 XAUT bid 卖出，以 PAXG ask 买入
        spread_short_xaut = xaut_bid - paxg_ask

        now = datetime.now().strftime("%H:%M:%S")
        print(f"[{now}] PAXG: {paxg_bid:.2f}/{paxg_ask:.2f} | XAUT: {xaut_bid:.2f}/{xaut_ask:.2f}")
        if paxg_quote_size != QUOTE_SIZE or xaut_quote_size != QUOTE_SIZE:
            print(f"        使用报价档位 PAXG: {paxg_quote_size} | XAUT: {xaut_quote_size}")
        print(f"        空PAXG多XAUT: ${spread_short_paxg:.2f} | 空XAUT多PAXG: ${spread_short_xaut:.2f}")
        print(f"        资金费率 PAXG: {paxg_fr_display} | XAUT: {xaut_fr_display}")

        # 检查是否需要通知
        current_time = time.monotonic()
        alert_sections = []
        alert_keys = []

        if spread_short_paxg > SPREAD_THRESHOLD:
            if current_time - last_alert_time["short_paxg"] > ALERT_COOLDOWN:
                alert_keys.append("short_paxg")
                alert_sections.append(
                    f"空PAXG多XAUT价差: <b>${spread_short_paxg:.2f}</b>\n"
                    f"PAXG bid: ${paxg_bid:.2f}\n"
                    f"XAUT ask: ${xaut_ask:.2f}\n\n"
                    f"资金费率:\n"
                    f"PAXG: {paxg_fr_display}\n"
                    f"XAUT: {xaut_fr_display}"
                )

        if spread_short_xaut > SPREAD_THRESHOLD:
            if current_time - last_alert_time["short_xaut"] > ALERT_COOLDOWN:
                alert_keys.append("short_xaut")
                alert_sections.append(
                    f"空XAUT多PAXG价差: <b>${spread_short_xaut:.2f}</b>\n"
                    f"XAUT bid: ${xaut_bid:.2f}\n"
                    f"PAXG ask: ${paxg_ask:.2f}\n\n"
                    f"资金费率:\n"
                    f"PAXG: {paxg_fr_display}\n"
                    f"XAUT: {xaut_fr_display}"
                )

        if alert_sections:
            print("        ⚠️  价差超过阈值，发送通知!")
            alert_msg = "🔔 <b>价差预警</b>\n\n" + "\n\n".join(alert_sections)
            send_telegram(alert_msg)
            for key in alert_keys:
                last_alert_time[key] = current_time

        print()
        time.sleep(CHECK_INTERVAL)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n监控已停止")
    except Exception as e:
        error_msg = f"{type(e).__name__}: {e}"
        print(f"\n脚本异常退出: {error_msg}")
        trace = traceback.format_exc()
        if len(trace) > 3000:
            trace = trace[-3000:]
        send_telegram(
            "⚠️ <b>脚本异常退出</b>\n\n"
            f"{error_msg}\n\n"
            f"{trace}"
        )
