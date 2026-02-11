# services/reaction_monitor.py
import aiohttp
import logging
import os
from fastapi import FastAPI, Request
import uvicorn
from typing import Optional

from services.previewer import PreviewerService

logger = logging.getLogger(__name__)

class ReactionMonitor:
    """Мониторинг реакций и обработка нажатий кнопок."""
    
    # 🔥 КОНСТАНТА ПОРТА
    WEBHOOK_PORT = 8081  # Можешь изменить здесь
    
    def __init__(self):
        self.bot_token = os.getenv('PUBLISH_API')
        self.previewer = PreviewerService()
        self.app = FastAPI(title="Telegram Webhook")
        self.setup_routes()
        
    def setup_routes(self):
        """Настройка маршрутов FastAPI."""
        
        @self.app.post("/webhook/telegram")
        async def telegram_webhook(request: Request):
            """Принимает вебхуки от Telegram (нажатия кнопок)."""
            try:
                update = await request.json()
                
                # Обработка нажатий кнопок
                if "callback_query" in update:
                    callback = update["callback_query"]
                    callback_data = callback.get("data", "")
                    message = callback.get("message", {})
                    message_id = message.get("message_id")
                    
                    # Берем caption или text
                    post_text = message.get("caption") or message.get("text") or ""
                    
                    # Если нет текста - берем из БД
                    if not post_text and self.previewer:
                        post_text = await self.previewer.get_caption_by_message_id(message_id) or ""
                    
                    # Определяем какая кнопка нажата
                    if callback_data.startswith("btn_image_"):
                        url = "https://n8n-tg-marcell88.amvera.io/webhook/35e1e741-9733-48b2-a335-2e3969368460"
                        button_name = "Картинка"
                    elif callback_data.startswith("btn_post_"):
                        url = "https://n8n-tg-marcell88.amvera.io/webhook/81fc81a9-3208-462a-a858-bc27c0460fdf"
                        button_name = "Пост"
                    else:
                        return {"ok": False, "error": "Unknown button"}
                    
                    # Отправляем POST на твой n8n сервер
                    payload = {
                        "id": message_id,
                        "text": post_text
                    }
                    
                    async with aiohttp.ClientSession() as session:
                        async with session.post(url, json=payload) as resp:
                            if resp.status in [200, 201, 202, 204]:
                                logger.info(f"✅ {button_name}: message_id={message_id}")
                            else:
                                text = await resp.text()
                                logger.error(f"❌ {button_name}: {resp.status} - {text}")
                    
                    # Отвечаем Telegram (убираем "часики" на кнопке)
                    async with aiohttp.ClientSession() as tg_session:
                        tg_url = f"https://api.telegram.org/bot{self.bot_token}/answerCallbackQuery"
                        await tg_session.post(tg_url, json={
                            "callback_query_id": callback.get("id"),
                            "text": "✅ Отправлено",
                            "show_alert": False
                        })
                    
                    return {"ok": True, "processed": "callback_query"}
                    
            except Exception as e:
                logger.error(f"Ошибка в webhook: {e}")
                return {"ok": False, "error": str(e)}
            
            return {"ok": True}
        
        @self.app.get("/health")
        async def health():
            return {"status": "ok", "service": "reaction_monitor"}
    
    async def run_monitoring(self):
        """Запускает вебхук сервер."""
        # 🔥 ИСПОЛЬЗУЕМ КОНСТАНТУ
        port = self.WEBHOOK_PORT
        logger.info(f"🚀 Reaction Monitor webhook запущен на порту {port}")
        
        config = uvicorn.Config(
            self.app,
            host="0.0.0.0",
            port=port,
            log_level="warning"
        )
        server = uvicorn.Server(config)
        await server.serve()