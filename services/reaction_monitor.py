# services/reaction_monitor.py
import aiohttp
import logging
import os
import time  # 👈 ДОБАВИТЬ
from fastapi import FastAPI, Request
import uvicorn
from typing import Optional
import json  # 👈 для красивого вывода

from services.previewer import PreviewerService

logger = logging.getLogger(__name__)

class ReactionMonitor:
    """Мониторинг реакций и обработка нажатий кнопок."""
    
    # КОНСТАНТА ПОРТА
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
            # 🔥 ЛОГИРУЕМ ВХОДЯЩИЙ ЗАПРОС
            logger.info("=" * 60)
            logger.info("📩 ПОЛУЧЕН ВЕБХУК ОТ TELEGRAM")
            
            try:
                # Получаем тело запроса
                body = await request.body()
                logger.info(f"📦 Raw body: {body.decode()[:500]}")  # первые 500 символов
                
                update = await request.json()
                logger.info(f"📋 Update: {json.dumps(update, indent=2, ensure_ascii=False)[:1000]}")
                
                # Обработка нажатий кнопок
                if "callback_query" in update:
                    callback = update["callback_query"]
                    callback_data = callback.get("data", "")
                    message = callback.get("message", {})
                    message_id = message.get("message_id")
                    
                    logger.info(f"🖱 НАЖАТА КНОПКА:")
                    logger.info(f"   └─ Callback data: {callback_data}")
                    logger.info(f"   └─ Message ID: {message_id}")
                    logger.info(f"   └─ User: {callback.get('from', {}).get('username', 'unknown')}")
                    
                    # Берем caption или text
                    post_text = message.get("caption") or message.get("text") or ""
                    logger.info(f"   └─ Текст поста: {post_text[:100]}..." if len(post_text) > 100 else f"   └─ Текст поста: {post_text}")
                    
                    # Если нет текста - берем из БД
                    if not post_text and self.previewer:
                        logger.info(f"🔍 Текст не найден, ищем в БД по message_id={message_id}")
                        post_text = await self.previewer.get_caption_by_message_id(message_id) or ""
                        logger.info(f"   └─ Текст из БД: {post_text[:100]}..." if len(post_text) > 100 else f"   └─ Текст из БД: {post_text}")
                    
                    # Определяем какая кнопка нажата
                    if callback_data.startswith("btn_image_"):
                        url = "https://n8n-tg-marcell88.amvera.io/webhook/35e1e741-9733-48b2-a335-2e3969368460"
                        button_name = "Картинка"
                        record_id = callback_data.replace("btn_image_", "")
                    elif callback_data.startswith("btn_post_"):
                        url = "https://n8n-tg-marcell88.amvera.io/webhook/81fc81a9-3208-462a-a858-bc27c0460fdf"
                        button_name = "Пост"
                        record_id = callback_data.replace("btn_post_", "")
                    else:
                        logger.warning(f"❓ Неизвестная кнопка: {callback_data}")
                        return {"ok": False, "error": "Unknown button"}
                    
                    logger.info(f"🎯 ОБРАБОТКА: {button_name}")
                    logger.info(f"   └─ Record ID: {record_id}")
                    logger.info(f"   └─ URL: {url}")
                    
                    # Отправляем POST на твой n8n сервер
                    payload = {
                        "id": message_id,
                        "text": post_text
                    }
                    
                    logger.info(f"📤 ОТПРАВКА НА n8n:")
                    logger.info(f"   └─ URL: {url}")
                    logger.info(f"   └─ Payload: {json.dumps(payload, ensure_ascii=False)}")
                    
                    start_time = time.time()
                    
                    async with aiohttp.ClientSession() as session:
                        async with session.post(url, json=payload) as resp:
                            response_time = time.time() - start_time
                            
                            if resp.status in [200, 201, 202, 204]:
                                logger.info(f"✅ УСПЕХ: {button_name}")
                                logger.info(f"   └─ Status: {resp.status}")
                                logger.info(f"   └─ Time: {response_time:.2f}s")
                                
                                # Пробуем получить ответ
                                try:
                                    resp_json = await resp.json()
                                    logger.info(f"   └─ Response: {json.dumps(resp_json, ensure_ascii=False)[:200]}")
                                except:
                                    resp_text = await resp.text()
                                    logger.info(f"   └─ Response: {resp_text[:200]}")
                                    
                            else:
                                logger.error(f"❌ ОШИБКА: {button_name}")
                                logger.error(f"   └─ Status: {resp.status}")
                                logger.error(f"   └─ Time: {response_time:.2f}s")
                                try:
                                    error_text = await resp.text()
                                    logger.error(f"   └─ Error: {error_text[:500]}")
                                except:
                                    pass
                    
                    # Отвечаем Telegram (убираем "часики" на кнопке)
                    logger.info(f"🔄 ОТВЕТ TELEGRAM:")
                    
                    answer_payload = {
                        "callback_query_id": callback.get("id"),
                        "text": "✅ Отправлено",
                        "show_alert": False
                    }
                    
                    async with aiohttp.ClientSession() as tg_session:
                        tg_url = f"https://api.telegram.org/bot{self.bot_token}/answerCallbackQuery"
                        async with tg_session.post(tg_url, json=answer_payload) as tg_resp:
                            if tg_resp.status == 200:
                                logger.info(f"   └─ ✅ Ответ отправлен, часики убраны")
                            else:
                                logger.error(f"   └─ ❌ Ошибка ответа Telegram: {tg_resp.status}")
                    
                    logger.info("=" * 60)
                    return {"ok": True, "processed": "callback_query"}
                    
            except Exception as e:
                logger.error(f"🔥 КРИТИЧЕСКАЯ ОШИБКА В WEBHOOK: {e}")
                logger.exception(e)  # 👈 полный stack trace
                return {"ok": False, "error": str(e)}
            
            logger.info("⏭ Не callback_query, пропускаем")
            logger.info("=" * 60)
            return {"ok": True}
        
        @self.app.get("/health")
        async def health():
            return {"status": "ok", "service": "reaction_monitor"}
    
    async def run_monitoring(self):
        """Запускает вебхук сервер."""
        port = self.WEBHOOK_PORT
        logger.info(f"🚀 Reaction Monitor webhook запущен на порту {port}")
        logger.info(f"📡 Endpoint: http://0.0.0.0:{port}/webhook/telegram")
        logger.info("=" * 60)
        
        config = uvicorn.Config(
            self.app,
            host="0.0.0.0",
            port=port,
            log_level="warning"
        )
        server = uvicorn.Server(config)
        await server.serve()