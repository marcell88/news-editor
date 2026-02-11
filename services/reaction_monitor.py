# services/reaction_monitor.py
import aiohttp
import logging
import os
from typing import Optional

from services.previewer import PreviewerService

logger = logging.getLogger(__name__)

class ReactionMonitor:
    """Мониторинг реакций и обработка нажатий кнопок."""
    
    def __init__(self):
        self.bot_token = os.getenv('PUBLISH_API')
        self.previewer = PreviewerService()
    
    async def process_callback(self, callback_data: str, message_id: int, post_text: str):
        """Обрабатывает нажатие кнопки и шлет в n8n."""
        
        # Определяем URL
        if callback_data.startswith("btn_image_"):
            url = "https://n8n-tg-marcell88.amvera.io/webhook/35e1e741-9733-48b2-a335-2e3969368460"
            button_name = "Картинка"
        elif callback_data.startswith("btn_post_"):
            url = "https://n8n-tg-marcell88.amvera.io/webhook/81fc81a9-3208-462a-a858-bc27c0460fdf"
            button_name = "Пост"
        else:
            return
        
        # Шлем в n8n
        payload = {"id": message_id, "text": post_text}
        
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload) as resp:
                if resp.status in [200, 201, 202, 204]:
                    logger.info(f"✅ {button_name}: message_id={message_id}")
                else:
                    logger.error(f"❌ {button_name}: {resp.status}")
    
    async def run_monitoring(self):
        """НИЧЕГО НЕ ДЕЛАЕТ, потому что вебхуки приходят в n8n напрямую."""
        logger.info("⏹ Reaction Monitor: вебхуки идут в n8n, свой сервер не запускаем")
        await asyncio.sleep(999999)  # или просто убери вызов этой функции из app.py