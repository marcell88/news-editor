# services/previewer.py (фоновая версия)
import asyncio
import logging
import base64
import os
import time
import aiohttp
import json  # 👈 ДОБАВЛЕНО
from datetime import datetime
from typing import List, Dict, Any, Optional
from dotenv import load_dotenv
from datetime import datetime
import pytz

from database.database import Database

logger = logging.getLogger(__name__)
load_dotenv()

class PreviewerService:
    """Служба для публикации записей в preview группу (фоновая)."""
    
    def __init__(self):
        self.bot_token = os.getenv('PUBLISH_API')
        self.preview_group = os.getenv('PREVIEW_GROUP')
        self.check_interval = 30  # Проверять каждые 30 секунд
        
        if not self.bot_token:
            raise ValueError("Не найден PUBLISH_API в .env")
        if not self.preview_group:
            raise ValueError("Не найден PREVIEW_GROUP в .env")
        
        logger.info(f"Previewer инициализирован для группы: {self.preview_group}")
    
    async def run_monitoring(self):
        """Фоновый цикл мониторинга и публикации."""
        try:
            logger.info("🚀 Previewer Service запущен (фоновая служба)")
            
            while True:
                await self._check_and_publish()
                await asyncio.sleep(self.check_interval)
                
        except asyncio.CancelledError:
            logger.info("Previewer Service остановлен")
        except Exception as e:
            logger.error(f"Ошибка в основном цикле Previewer: {e}")
            await asyncio.sleep(60)
    
    async def _check_and_publish(self):
        """Проверяет и публикует записи."""
        try:
            # Инициализируем БД если еще не инициализирована
            try:
                pool = await Database.get_pool()
            except:
                await Database.initialize_database()
                pool = await Database.get_pool()
            
            # Берем записи для preview
            records = await self._get_records_for_preview(pool)
            
            if not records:
                logger.debug("Нет записей для preview")
                return
            
            logger.info(f"📋 Найдено {len(records)} записей для preview")
            
            # Публикуем каждую запись
            for i, record in enumerate(records):
                success = await self._publish_record(record, i+1, len(records))
                
                if success:
                    # Обновляем запись в БД
                    await self._mark_as_previewed(pool, record['id'])
                else:
                    logger.error(f"❌ Ошибка публикации записи ID {record['id']}")
                
                # Пауза между публикациями (кроме последней)
                if i < len(records) - 1:
                    await asyncio.sleep(10)
            
        except Exception as e:
            logger.error(f"Ошибка в _check_and_publish: {e}")
    
    async def _get_records_for_preview(self, pool) -> List[Dict[str, Any]]:
        """Получает записи для публикации в preview."""
        try:
            async with pool.acquire() as conn:
                # Ищем записи с готовыми текстом и изображением, но еще не отправленные в preview
                query = """
                SELECT id, text_prepared, "pic-base64", time, final_score
                FROM to_publish 
                WHERE "pic-base64" IS NOT NULL 
                  AND text_prepared IS NOT NULL
                  AND preview = false
                  AND prepare = true
                  AND LENGTH("pic-base64") > 100
                  AND LENGTH(text_prepared) > 10
                ORDER BY id ASC
                LIMIT 5
                """
                
                rows = await conn.fetch(query)
                return [dict(row) for row in rows]
                
        except Exception as e:
            logger.error(f"❌ Ошибка получения записей для preview: {e}")
            return []
    
    async def _publish_record(self, record: Dict, current: int, total: int) -> bool:
        """Публикует одну запись в preview группу."""
        try:
            record_id = record['id']
            text_prepared = record['text_prepared']
            pic_base64 = record['pic-base64']
            publish_time = record.get('time')
            final_score = record.get('final_score')
            
            logger.info(f"📤 Публикация {current}/{total}: ID {record_id}")
            
            # Добавляем [ID], final_score и время публикации в текст
            caption = self._add_metadata_to_caption(
                text_prepared, 
                record_id, 
                publish_time,
                final_score
            )
            
            # Отправляем в Telegram
            success = await self._send_to_telegram(pic_base64, caption, record_id)
            
            return success
            
        except Exception as e:
            logger.error(f"❌ Ошибка публикации записи ID {record['id']}: {e}")
            return False
    
    def _format_publish_date(self, unix_timestamp: int) -> str:
        """Форматирует UNIX-время в строку вида '01.02.2026, 13:23' по Москве."""
        try:
            if not unix_timestamp:
                return ""
            
            # Создаем UTC время
            utc_dt = datetime.utcfromtimestamp(unix_timestamp)
            utc_dt = utc_dt.replace(tzinfo=pytz.utc)
            
            # Конвертируем в московское время
            moscow_tz = pytz.timezone('Europe/Moscow')
            moscow_dt = utc_dt.astimezone(moscow_tz)
            
            return moscow_dt.strftime("%d.%m.%Y, %H:%M")
        except Exception as e:
            logger.error(f"Ошибка форматирования времени {unix_timestamp}: {e}")
            return ""
    
    def _escape_markdown(self, text: str) -> str:
        """Экранирует специальные символы для Markdown V2."""
        if not text:
            return ""
        
        special_chars = ['\\', '_', '*', '[', ']', '(', ')', '~', '`',
                        '>', '<', '&', '#', '+', '-', '=', '|', '{',
                        '}', '.', '!', ',', ':']
        
        for char in special_chars:
            text = text.replace(char, f'\\{char}')
        return text
    
    def _add_metadata_to_caption(self, caption: str, record_id: int, 
                                 publish_time: int = None, final_score: float = None) -> str:
        """Добавляет [ID], final_score и время публикации в текст."""
        # Добавляем ID в конец текста
        result = f"{caption}\n\n\\[ID\\] {record_id}"
        
        # Добавляем final_score, если он есть
        if final_score is not None:
            # Округляем до десятков
            rounded_score = round(final_score, 1)
            # Экранируем для Markdown V2
            escaped_score = self._escape_markdown(f"{rounded_score}")
            result += f"\n\n\\=\\=\\=\n\nОценка\\: {escaped_score}"
        
        # Добавляем время публикации, если оно есть
        if publish_time:
            formatted_time = self._format_publish_date(publish_time)
            if formatted_time:
                escaped_time = self._escape_markdown(formatted_time)
                if final_score is not None:
                    result += f"\n\n{escaped_time}"
                else:
                    result += f"\n\n\\=\\=\\=\n\n{escaped_time}"
        
        return result
            
    async def _send_to_telegram(self, pic_base64: str, caption: str, record_id: int) -> bool:
        """Отправляет фото с текстом и двумя кнопками в Telegram."""
        try:
            # Декодируем изображение
            photo_data = base64.b64decode(pic_base64)
            
            # 👇 ПРОСТЫЕ КНОПКИ - КАРТИНКА СВЕРХУ, ПОСТ СНИЗУ
            keyboard = {
                "inline_keyboard": [
                    [{"text": "Отправить", "callback_data": f"btn_publish_{record_id}"}],
                    [{"text": "Картинка", "callback_data": f"btn_image_{record_id}"}],
                    [{"text": "Пост", "callback_data": f"btn_post_{record_id}"}]
                ]
            }
            
            # Формируем запрос
            form = aiohttp.FormData()
            form.add_field('chat_id', self.preview_group)
            form.add_field('caption', caption)  # БЕЗ лишних переносов
            form.add_field('parse_mode', 'MarkdownV2')
            form.add_field('reply_markup', json.dumps(keyboard))
            form.add_field('photo', photo_data, filename='image.jpg', content_type='image/jpeg')
            
            url = f"https://api.telegram.org/bot{self.bot_token}/sendPhoto"
            
            async with aiohttp.ClientSession() as session:
                async with session.post(url, data=form) as response:
                    result = await response.json()
                    
                    if response.status == 200 and result.get('ok'):
                        message_id = result['result']['message_id']
                        logger.info(f"✅ Отправлено! ID записи: {record_id}, ID сообщения: {message_id}")
                        
                        # Сохраняем связь для обработчика кнопок
                        await self._save_message_mapping(message_id, record_id, caption)
                        
                        return True
                    else:
                        logger.error(f"❌ Ошибка отправки записи {record_id}: {result}")
                        return False
                        
        except Exception as e:
            logger.error(f"❌ Ошибка при отправке в Telegram: {e}")
            return False
    
    async def _save_message_mapping(self, message_id: int, record_id: int, caption: str):
        """Сохраняет соответствие message_id и record_id в БД."""
        try:
            pool = await Database.get_pool()
            async with pool.acquire() as conn:
                # Создаем таблицу если нет
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS message_mapping (
                        message_id BIGINT PRIMARY KEY,
                        record_id INTEGER NOT NULL,
                        caption TEXT,
                        created_at INTEGER NOT NULL
                    )
                """)
                
                current_time = int(time.time())
                await conn.execute("""
                    INSERT INTO message_mapping (message_id, record_id, caption, created_at)
                    VALUES ($1, $2, $3, $4)
                    ON CONFLICT (message_id) DO UPDATE 
                    SET record_id = $2, caption = $3, created_at = $4
                """, message_id, record_id, caption, current_time)
                
                logger.debug(f"💾 Сохранена связь message_id={message_id} -> record_id={record_id}")
        except Exception as e:
            logger.error(f"❌ Ошибка сохранения маппинга: {e}")
    
    async def get_caption_by_message_id(self, message_id: int) -> Optional[str]:
        """Получает текст поста по message_id из БД."""
        try:
            pool = await Database.get_pool()
            async with pool.acquire() as conn:
                row = await conn.fetchrow("""
                    SELECT caption FROM message_mapping WHERE message_id = $1
                """, message_id)
                
                return row['caption'] if row else None
                
        except Exception as e:
            logger.error(f"❌ Ошибка получения caption по message_id {message_id}: {e}")
            return None
    
    async def _mark_as_previewed(self, pool, record_id: int):
        """Помечает запись как отправленную в preview и записывает время."""
        try:
            current_time = int(time.time())
            
            async with pool.acquire() as conn:
                try:
                    query = """
                    UPDATE to_publish 
                    SET 
                        preview = true,
                        previewed_at = $1
                    WHERE id = $2
                    """
                    await conn.execute(query, current_time, record_id)
                except Exception as e:
                    logger.warning(f"Столбец previewed_at не найден, обновляем только preview: {e}")
                    query = """
                    UPDATE to_publish 
                    SET preview = true
                    WHERE id = $1
                    """
                    await conn.execute(query, record_id)
                
                logger.info(f"✅ Запись ID {record_id} помечена как previewed")
                
        except Exception as e:
            logger.error(f"❌ Ошибка обновления записи ID {record_id}: {e}")
            raise


async def main():
    """Тестовый запуск службы."""
    previewer = PreviewerService()
    await previewer.run_monitoring()

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    if not os.getenv('PUBLISH_API') or not os.getenv('PREVIEW_GROUP'):
        print("❌ Отсутствуют обязательные переменные в .env:")
        print("   PUBLISH_API=ваш_токен_бота")
        print("   PREVIEW_GROUP=ваш_id_группы")
        exit(1)
    
    asyncio.run(main())