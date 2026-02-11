# services/reaction_monitor.py
import aiohttp
import logging
import os
import json
from fastapi import FastAPI, Request
import uvicorn
import threading
from typing import Optional

from services.previewer import PreviewerService

logger = logging.getLogger(__name__)

class ReactionMonitor:
    """Обработчик нажатий кнопок через вебхук."""
    
    WEBHOOK_PORT = 8081  # Порт для вебхуков
    
    def __init__(self):
        self.bot_token = os.getenv('PUBLISH_API')
        self.previewer = PreviewerService()
        self.app = FastAPI(title="Telegram Callback Webhook")
        self.setup_routes()
        self.server_thread = None
    
    def setup_routes(self):
        """Настройка маршрутов FastAPI."""
        
        @self.app.post("/webhook/telegram")
        async def telegram_webhook(request: Request):
            """Принимает нажатия кнопок от Telegram."""
            try:
                update = await request.json()
                logger.info(f"📩 Получен вебхук: {json.dumps(update, ensure_ascii=False)[:200]}")
                
                if "callback_query" in update:
                    callback = update["callback_query"]
                    callback_data = callback.get("data", "")
                    message = callback.get("message", {})
                    message_id = message.get("message_id")
                    
                    # Берем текст (caption или text)
                    post_text = message.get("caption") or message.get("text") or ""
                    
                    # Если текста нет - пробуем из БД
                    if not post_text and self.previewer:
                        post_text = await self.previewer.get_caption_by_message_id(message_id) or ""
                    
                    # Определяем кнопку и URL
                    if callback_data.startswith("btn_image_"):
                        url = "https://n8n-tg-marcell88.amvera.io/webhook/35e1e741-9733-48b2-a335-2e3969368460"
                        button_name = "Картинка"
                    elif callback_data.startswith("btn_post_"):
                        url = "https://n8n-tg-marcell88.amvera.io/webhook/81fc81a9-3208-462a-a858-bc27c0460fdf"
                        button_name = "Пост"
                    else:
                        logger.warning(f"Неизвестная кнопка: {callback_data}")
                        return {"ok": False}
                    
                    # Отправляем в n8n
                    payload = {
                        "id": message_id,
                        "text": post_text
                    }
                    
                    logger.info(f"➡️ {button_name}: message_id={message_id}")
                    
                    async with aiohttp.ClientSession() as session:
                        async with session.post(url, json=payload) as resp:
                            if resp.status in [200, 201, 202, 204]:
                                logger.info(f"✅ {button_name} отправлен: {resp.status}")
                            else:
                                logger.error(f"❌ {button_name} ошибка: {resp.status}")
                    
                    # Отвечаем Telegram (убираем часики)
                    async with aiohttp.ClientSession() as tg_session:
                        tg_url = f"https://api.telegram.org/bot{self.bot_token}/answerCallbackQuery"
                        await tg_session.post(tg_url, json={
                            "callback_query_id": callback.get("id"),
                            "text": "✅ Отправлено",
                            "show_alert": False
                        })
                    
                    return {"ok": True}
                    
            except Exception as e:
                logger.error(f"❌ Ошибка: {e}")
                logger.exception(e)
            
            return {"ok": True}
        
        @self.app.get("/health")
        async def health():
            return {"status": "ok", "service": "reaction_monitor"}
    
    def run_server(self):
        """Запускает FastAPI сервер в отдельном потоке."""
        logger.info(f"🚀 Запуск вебхук сервера на порту {self.WEBHOOK_PORT}")
        uvicorn.run(self.app, host="0.0.0.0", port=self.WEBHOOK_PORT, log_level="warning")
    
    async def run_monitoring(self):
        """Запускает обработчик в фоне."""
        logger.info("🔄 Reaction Monitor инициализирован")
        
        # Запускаем FastAPI в отдельном потоке
        import threading
        self.server_thread = threading.Thread(target=self.run_server, daemon=True)
        self.server_thread.start()
        
        logger.info(f"✅ Вебхук сервер запущен на порту {self.WEBHOOK_PORT}")
        logger.info(f"📡 Endpoint: http://0.0.0.0:{self.WEBHOOK_PORT}/webhook/telegram")
        logger.info("⏳ Ожидание нажатий кнопок...")
        
        # Держим задачу активной
        while True:
            await asyncio.sleep(60)