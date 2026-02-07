from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import requests


@dataclass(slots=True)
class BotCommand:
    chat_id: str
    user_id: str
    text: str


class TelegramBot:
    def __init__(self, token: str | None, allowed_chat_ids: set[str] | None = None) -> None:
        self.token = token
        self.allowed_chat_ids = allowed_chat_ids or set()

    @property
    def enabled(self) -> bool:
        return bool(self.token)

    def update_allowed_chat_ids(self, allowed_chat_ids: set[str]) -> None:
        self.allowed_chat_ids = allowed_chat_ids

    def send_to_allowed(self, message: str, parse_mode: str = "HTML") -> None:
        for chat_id in self.allowed_chat_ids:
            self.send_message(chat_id, message, parse_mode=parse_mode)

    def send_message(self, chat_id: str, message: str, parse_mode: str = "HTML") -> None:
        if not self.enabled:
            return
        url = f"https://api.telegram.org/bot{self.token}/sendMessage"
        payload = {
            "chat_id": chat_id,
            "text": message,
            "parse_mode": parse_mode,
            "disable_web_page_preview": True,
        }
        requests.post(url, data=payload, timeout=10)

    def poll_commands(self, offset: int | None = None, timeout_sec: int = 0) -> tuple[list[BotCommand], int | None]:
        if not self.enabled:
            return [], offset

        url = f"https://api.telegram.org/bot{self.token}/getUpdates"
        params: dict[str, Any] = {
            "timeout": timeout_sec,
            "allowed_updates": ["message"],
        }
        if offset is not None:
            params["offset"] = offset

        resp = requests.get(url, params=params, timeout=max(timeout_sec + 2, 5))
        resp.raise_for_status()

        data = resp.json()
        if not isinstance(data, dict) or not data.get("ok"):
            return [], offset

        commands: list[BotCommand] = []
        next_offset = offset
        for update in data.get("result", []):
            if not isinstance(update, dict):
                continue
            update_id = update.get("update_id")
            if isinstance(update_id, int):
                next_offset = update_id + 1

            message = update.get("message")
            if not isinstance(message, dict):
                continue
            text = message.get("text")
            if not isinstance(text, str) or not text.startswith("/"):
                continue
            chat = message.get("chat") or {}
            user = message.get("from") or {}
            chat_id = str(chat.get("id", ""))
            user_id = str(user.get("id", ""))
            if not chat_id:
                continue
            commands.append(BotCommand(chat_id=chat_id, user_id=user_id, text=text.strip()))

        return commands, next_offset

    def is_authorized(self, chat_id: str) -> bool:
        if not self.allowed_chat_ids:
            return False
        return chat_id in self.allowed_chat_ids
