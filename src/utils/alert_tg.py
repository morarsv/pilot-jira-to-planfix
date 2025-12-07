import logging
from telegram import Bot
from telegram.error import TelegramError
from src.configuration.config import Settings

logger = logging.getLogger(__name__)
settings = Settings()
bot = Bot(token=settings.BOT_TOKEN)

async def send_alert_to_chat(text: str,
                             chat_id: str) -> None:
    try:
        await bot.send_message(chat_id=chat_id,
                               text=text)
    except TelegramError as e:
        logger.error(f"Не удалось отправить сообщение в Telegram: {e}")
