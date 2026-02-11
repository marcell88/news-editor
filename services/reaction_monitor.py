# services/button_handler.py
import aiohttp
import logging
import json
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)

class ButtonHandler:
    """Обработчик нажатий на кнопки Картинка и Пост."""
    
    # URL для POST запросов
    IMAGE_WEBHOOK_URL = "https://n8n-tg-marcell88.amvera.io/webhook/35e1e741-9733-48b2-a335-2e3969368460"
    POST_WEBHOOK_URL = "https://n8n-tg-marcell88.amvera.io/webhook/81fc81a9-3208-462a-a858-bc27c0460fdf"
    
    @classmethod
    async def process_callback(cls, callback_data: str, message_id: int, post_text: str) -> bool:
        """
        Обрабатывает нажатие на кнопку.
        
        Args:
            callback_data: данные кнопки (btn_image_123 или btn_post_123)
            message_id: ID сообщения в Telegram
            post_text: текст поста (caption или text)
        
        Returns:
            bool: успех отправки
        """
        try:
            # Определяем какая кнопка нажата
            if callback_data.startswith("btn_image_"):
                url = cls.IMAGE_WEBHOOK_URL
                button_type = "Картинка"
            elif callback_data.startswith("btn_post_"):
                url = cls.POST_WEBHOOK_URL
                button_type = "Пост"
            else:
                logger.warning(f"Неизвестный callback_data: {callback_data}")
                return False
            
            # Формируем payload
            payload = {
                "id": message_id,      # ID сообщения как number
                "text": post_text      # caption или text
            }
            
            logger.info(f"📤 Отправка вебхука для '{button_type}': message_id={message_id}")
            logger.debug(f"Payload: {payload}")
            
            # Отправляем POST запрос
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    url, 
                    json=payload,
                    headers={"Content-Type": "application/json"}
                ) as response:
                    
                    if response.status in [200, 201, 202, 204]:
                        logger.info(f"✅ Вебхук для '{button_type}' отправлен: {response.status}")
                        return True
                    else:
                        text = await response.text()
                        logger.error(f"❌ Ошибка вебхука для '{button_type}': {response.status} - {text}")
                        return False
                        
        except Exception as e:
            logger.error(f"❌ Ошибка отправки вебхука: {e}")
            return False


class CallbackHandler:
    """Обработчик callback запросов от Telegram."""
    
    def __init__(self, bot_token: str, previewer_service=None):
        self.bot_token = bot_token
        self.previewer = previewer_service  # для получения текста из БД если надо
    
    async def handle(self, callback_query: Dict[str, Any]) -> Dict[str, Any]:
        """
        Обрабатывает callback_query от Telegram.
        
        Args:
            callback_query: объект callback_query из Telegram API
        
        Returns:
            Dict: ответ для Telegram API
        """
        try:
            # Извлекаем данные
            callback_data = callback_query.get("data", "")
            message = callback_query.get("message", {})
            message_id = message.get("message_id")
            
            # 🔥 Берем caption, если нет - берем text, если нет - пустую строку
            post_text = message.get("caption") or message.get("text") or ""
            
            # Если текст пустой - пробуем достать из БД через previewer
            if not post_text and self.previewer:
                post_text = await self.previewer.get_caption_by_message_id(message_id) or ""
                logger.info(f"📦 Текст получен из БД: message_id={message_id}")
            
            # Отправляем вебхук
            success = await ButtonHandler.process_callback(
                callback_data=callback_data,
                message_id=message_id,
                post_text=post_text
            )
            
            # Формируем ответ для Telegram (убираем "часики" на кнопке)
            response_text = "✅ Отправлено" if success else "❌ Ошибка"
            
            return {
                "callback_query_id": callback_query.get("id"),
                "text": response_text,
                "show_alert": False  # True если хочешь всплывающее окно
            }
            
        except Exception as e:
            logger.error(f"❌ Ошибка обработки callback: {e}")
            return {
                "callback_query_id": callback_query.get("id"),
                "text": "❌ Ошибка",
                "show_alert": False
            }


# FastAPI webhook handler (если используешь FastAPI)
async def telegram_webhook_handler(request, previewer_service=None):
    """Обработчик вебхука от Telegram."""
    try:
        update = await request.json()
        
        # Проверяем, что это callback_query (нажатие кнопки)
        if "callback_query" in update:
            callback_query = update["callback_query"]
            
            # Создаем обработчик
            bot_token = os.getenv('PUBLISH_API')  # или другой токен
            handler = CallbackHandler(bot_token, previewer_service)
            
            # Обрабатываем нажатие
            answer = await handler.handle(callback_query)
            
            # Отвечаем на callback_query
            async with aiohttp.ClientSession() as session:
                url = f"https://api.telegram.org/bot{bot_token}/answerCallbackQuery"
                await session.post(url, json=answer)
            
            return {"ok": True, "message": "Callback processed"}
        
        return {"ok": True, "message": "Not a callback"}
        
    except Exception as e:
        logger.error(f"Ошибка в webhook: {e}")
        return {"ok": False, "error": str(e)}