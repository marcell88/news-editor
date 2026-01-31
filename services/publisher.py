# services/publisher.py
import asyncio
import logging
import base64
import os
import time
import aiohttp
from typing import List, Dict, Any, Optional
from dotenv import load_dotenv

from database.database import Database

logger = logging.getLogger(__name__)
load_dotenv()

# Константы
PUBLISH_INTERVAL = 1800  # 30 минут в секундах
CHECK_INTERVAL = 60      # Проверять каждую минуту

class PublisherService:
    """
    Служба для прямой публикации записей в Telegram-группу
    """
    
    def __init__(self):
        self.bot_token = os.getenv('PUBLISH_API')
        self.tg_group = os.getenv('TG_GROUP')
        
        if not self.bot_token:
            raise ValueError("Не найден PUBLISH_API в .env")
        if not self.tg_group:
            raise ValueError("Не найден TG_GROUP в .env")
        
        logger.info(f"Publisher инициализирован для группы: {self.tg_group}")
    
    async def run_monitoring(self):
        """Фоновый цикл мониторинга и публикации."""
        try:
            logger.info("🚀 Publisher Service запущен")
            
            while True:
                await self._check_and_publish()
                await asyncio.sleep(CHECK_INTERVAL)
                
        except asyncio.CancelledError:
            logger.info("Publisher Service остановлен")
        except Exception as e:
            logger.error(f"Ошибка в Publisher: {e}")
            await asyncio.sleep(60)
    
    async def _check_and_publish(self):
        """Проверяет и публикует записи."""
        try:
            pool = await Database.get_pool()
            
            # Получаем записи для публикации
            records = await self._get_records_for_publishing(pool)
            
            if not records:
                logger.debug("Нет записей для публикации")
                return
            
            logger.info(f"📋 Найдено {len(records)} записей для публикации")
            
            # Публикуем все записи по очереди
            for i, record in enumerate(records):
                is_last_record = (i == len(records) - 1)
                next_flag = not is_last_record
                
                success = await self._publish_record(record, pool, next_flag)
                
                if not success:
                    logger.error(f"❌ Ошибка публикации ID {record['id']}")
                    break
                
                # Ждем интервал, если это не последняя запись
                if not is_last_record:
                    logger.info(f"⏳ Жду {PUBLISH_INTERVAL // 60} минут...")
                    await asyncio.sleep(PUBLISH_INTERVAL)
                else:
                    logger.info("🎯 Последняя запись в очереди (next=false)")
            
        except Exception as e:
            logger.error(f"Ошибка в _check_and_publish: {e}")
    
    async def _get_records_for_publishing(self, pool) -> List[Dict[str, Any]]:
        """Получает записи для публикации (published = false)."""
        try:
            async with pool.acquire() as conn:
                current_time = int(time.time())
                
                query = """
                SELECT id, text, author, topic, mood, names, length, 
                       text_prepared, "pic-base64"
                FROM to_publish 
                WHERE published = false
                  AND pic = true 
                  AND "pic-base64" IS NOT NULL 
                  AND LENGTH("pic-base64") > 100
                  AND prepare = true
                  AND text_prepared IS NOT NULL
                  AND LENGTH(text_prepared) > 10
                  AND time <= $1
                ORDER BY id ASC
                LIMIT 10
                """
                
                rows = await conn.fetch(query, current_time)
                return [dict(row) for row in rows]
                
        except Exception as e:
            logger.error(f"❌ Ошибка получения записей: {e}")
            return []
    
    async def _publish_record(self, record: Dict, pool, next_flag: bool) -> bool:
        """Публикует запись в Telegram."""
        try:
            record_id = record['id']
            logger.info(f"📤 Публикация ID {record_id}")
            
            # Отправляем в Telegram
            success = await self._send_to_telegram(record)
            
            if not success:
                return False
            
            # Копируем в published
            await self._copy_to_published(pool, record, next_flag)
            
            # Помечаем как опубликованную В to_publish
            await self._mark_as_published(pool, record_id)
            
            logger.info(f"✅ Опубликовано ID {record_id}, next={next_flag}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Ошибка публикации ID {record['id']}: {e}")
            return False
    
    async def _send_to_telegram(self, record: Dict) -> bool:
        """Отправляет фото с текстом в Telegram."""
        try:
            caption = record['text_prepared']
            pic_base64 = record['pic-base64']
            
            photo_data = base64.b64decode(pic_base64)
            
            form = aiohttp.FormData()
            form.add_field('chat_id', self.tg_group)
            form.add_field('caption', caption)
            form.add_field('parse_mode', 'MarkdownV2')
            form.add_field('photo', photo_data, filename='image.jpg', content_type='image/jpeg')
            
            url = f"https://api.telegram.org/bot{self.bot_token}/sendPhoto"
            
            async with aiohttp.ClientSession() as session:
                async with session.post(url, data=form) as response:
                    result = await response.json()
                    
                    if response.status == 200 and result.get('ok'):
                        logger.debug(f"✅ Отправлено в Telegram")
                        return True
                    else:
                        logger.error(f"❌ Ошибка Telegram: {result}")
                        return False
                        
        except Exception as e:
            logger.error(f"❌ Ошибка отправки: {e}")
            return False
    
    async def _copy_to_published(self, pool, record: Dict, next_flag: bool):
        """Копирует запись в таблицу published."""
        try:
            current_time = int(time.time())
            
            async with pool.acquire() as conn:
                # Вставляем в published
                query = """
                INSERT INTO published 
                (text, author, topic, mood, names, length, published, next)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                """
                
                await conn.execute(
                    query,
                    record['text'],
                    record['author'],
                    record['topic'],
                    record['mood'],
                    record['names'],
                    record['length'],
                    current_time,
                    next_flag
                )
                
                logger.debug(f"Скопировано в published ID {record['id']}")
                
        except Exception as e:
            logger.error(f"❌ Ошибка копирования: {e}")
            raise
    
    async def _mark_as_published(self, pool, record_id: int):
        """Помечаем запись как опубликованную в to_publish."""
        try:
            async with pool.acquire() as conn:
                # ТОЛЬКО меняем published = true
                await conn.execute("""
                UPDATE to_publish 
                SET published = true
                WHERE id = $1
                """, record_id)
                
                logger.debug(f"Помечено как опубликованное ID {record_id}")
                
        except Exception as e:
            logger.error(f"❌ Ошибка обновления: {e}")
            raise


async def main():
    """Запуск службы."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    publisher = PublisherService()
    
    try:
        await Database.initialize_database()
        logger.info("✅ База данных готова")
    except Exception as e:
        logger.error(f"❌ Ошибка БД: {e}")
        return
    
    await publisher.run_monitoring()

if __name__ == "__main__":
    # Проверка переменных
    if not os.getenv('PUBLISH_API'):
        print("❌ Нет PUBLISH_API в .env")
        exit(1)
    
    if not os.getenv('TG_GROUP'):
        print("❌ Нет TG_GROUP в .env")
        exit(1)
    
    asyncio.run(main())