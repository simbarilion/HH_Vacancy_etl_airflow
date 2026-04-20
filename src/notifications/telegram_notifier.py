import asyncio
import os

import httpx
from dotenv import load_dotenv

from src.logging_config import LoggingConfigClassMixin

load_dotenv()


class TelegramNotifier(LoggingConfigClassMixin):
    """Простой отправщик уведомлений в Telegram"""

    def __init__(self):
        super().__init__()
        self.token = os.getenv("TELEGRAM_BOT_TOKEN")
        self.chat_id = os.getenv("TELEGRAM_CHAT_ID")
        self.logger = self.configure()

        if not self.token or not self.chat_id:
            self.logger.warning("Telegram токен или chat_id не настроены. Уведомления отключены")

    async def send_message(self, message: str, parse_mode: str = "HTML") -> None:
        """Отправляет сообщение в Telegram"""
        if not self.token or not self.chat_id:
            return

        url = f"https://api.telegram.org/bot{self.token}/sendMessage"

        payload = {
            "chat_id": self.chat_id,
            "text": message,
            "parse_mode": parse_mode,
            "disable_web_page_preview": True,
        }

        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                resp = await client.post(url, json=payload)
                if resp.status_code != 200:
                    self.logger.error(f"Telegram error: {resp.text}")
        except Exception as e:
            self.logger.error(f"Не удалось отправить сообщение в Telegram: {e}")

    def send_message_sync(self, message: str):
        """Синхронная версия для использования в Airflow callbacks"""
        asyncio.run(self.send_message(message))
