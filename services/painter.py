# services/painter.py
import asyncio
import logging
import base64
import aiohttp
import json
from typing import List, Dict, Any
from dotenv import load_dotenv

from database.database import Database

logger = logging.getLogger(__name__)
load_dotenv()

class PainterService:
    """
    Служба для генерации изображений на основе текста и сохранения в БД.
    Работает с таблицей to_publish, обновляя записи с pic = false.
    """
    
    def __init__(self):
        self.check_interval = 10  # секунд между проверками
        self.webhook_url = "https://process-app-marcell88.amvera.io/webhook/7a98c38a-61de-47f7-a606-9a330a194f0d"
        self.max_retries = 3
        self.retry_delay = 2  # секунды
        
    async def run_monitoring(self):
        """Основной цикл мониторинга."""
        try:
            logger.info("🎨 Painter Service запущен")
            
            while True:
                await self._check_and_process()
                await asyncio.sleep(self.check_interval)
                
        except asyncio.CancelledError:
            logger.info("Painter Service остановлен")
        except Exception as e:
            logger.error(f"Ошибка в основном цикле Painter: {e}")
            # Пауза перед перезапуском при критической ошибке
            await asyncio.sleep(30)
    
    async def _check_and_process(self):
        """Проверяет и обрабатывает записи, требующие генерации изображений."""
        try:
            pool = await Database.get_pool()
            
            # Получаем записи, где pic = false
            records = await self._get_records_to_process(pool)
            if records:
                logger.info(f"Найдено {len(records)} записей для генерации изображений")
                await self._process_records(pool, records)
                
        except Exception as e:
            logger.error(f"Ошибка в _check_and_process: {e}")
    
    async def _get_records_to_process(self, pool) -> List[Dict]:
        """Получает записи, требующие генерации изображений."""
        try:
            async with pool.acquire() as conn:
                query = """
                SELECT id, text 
                FROM to_publish 
                WHERE pic = false 
                ORDER BY id ASC
                LIMIT 5
                """
                rows = await conn.fetch(query)
                return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"Ошибка получения записей из to_publish: {e}")
            return []
    
    async def _process_records(self, pool, records: List[Dict]):
        """Обрабатывает записи: генерирует изображения и сохраняет в БД."""
        processed = 0
        failed = 0
        
        for record in records:
            try:
                record_id = record['id']
                text = record['text']
                
                if not text or not text.strip():
                    logger.warning(f"ID {record_id}: пустой текст, пропускаем")
                    # Помечаем как обработанное, но без изображения
                    await self._mark_as_processed(pool, record_id, None)
                    continue
                
                logger.info(f"ID {record_id}: генерация изображения для текста: {text[:50]}...")
                
                # Генерируем изображение
                image_base64 = await self._generate_image(text)
                
                if image_base64:
                    # Сохраняем в БД
                    await self._update_record(pool, record_id, image_base64)
                    processed += 1
                    logger.info(f"✅ ID {record_id}: изображение сгенерировано и сохранено")
                else:
                    # Если не удалось сгенерировать, помечаем как обработанное
                    await self._mark_as_processed(pool, record_id, None)
                    failed += 1
                    logger.error(f"❌ ID {record_id}: не удалось сгенерировать изображение")
                    
            except Exception as e:
                failed += 1
                logger.error(f"❌ ID {record['id']}: ошибка обработки: {e}")
        
        if processed or failed:
            logger.info(f"🎨 Обработано: {processed} успешно, {failed} с ошибками")
    
    async def _generate_image(self, text: str) -> str:
        """Генерирует изображение через webhook."""
        for attempt in range(self.max_retries):
            try:
                async with aiohttp.ClientSession() as session:
                    headers = {
                        'Content-Type': 'application/json',
                        'User-Agent': 'PainterService/1.0'
                    }
                    
                    payload = {
                        'text': text.strip()
                    }
                    
                    logger.debug(f"Отправка запроса к webhook: {text[:30]}...")
                    
                    async with session.post(
                        self.webhook_url, 
                        headers=headers, 
                        json=payload
                    ) as response:
                        
                        if response.status == 200:
                            # Получаем бинарные данные изображения
                            image_data = await response.read()
                            
                            if not image_data:
                                logger.error("Получен пустой ответ от сервера")
                                if attempt < self.max_retries - 1:
                                    await asyncio.sleep(self.retry_delay)
                                    continue
                                return ""
                            
                            # Конвертируем в base64
                            image_base64 = base64.b64encode(image_data).decode('utf-8')
                            
                            # Проверяем, что это валидный base64
                            if len(image_base64) > 100:  # Минимальная длина для изображения
                                return image_base64
                            else:
                                logger.error(f"Слишком короткий base64: {len(image_base64)} символов")
                                return ""
                        
                        else:
                            logger.error(f"Ошибка HTTP {response.status}: {await response.text()}")
                            if response.status == 429 and attempt < self.max_retries - 1:
                                # Too Many Requests - увеличиваем задержку
                                await asyncio.sleep(self.retry_delay * (attempt + 1))
                                continue
                            elif response.status >= 500 and attempt < self.max_retries - 1:
                                # Server errors - retry
                                await asyncio.sleep(self.retry_delay)
                                continue
                            return ""
                            
            except aiohttp.ClientError as e:
                logger.error(f"Ошибка сети при попытке {attempt + 1}: {e}")
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(self.retry_delay)
                    continue
                return ""
                
            except Exception as e:
                logger.error(f"Неожиданная ошибка при генерации изображения: {e}")
                return ""
        
        return ""
    
    async def _update_record(self, pool, record_id: int, image_base64: str):
        """Обновляет запись в БД с изображением."""
        try:
            async with pool.acquire() as conn:
                query = """
                UPDATE to_publish 
                SET 
                    "pic-base64" = $1,
                    pic = true
                WHERE id = $2
                """
                await conn.execute(query, image_base64, record_id)
        except Exception as e:
            logger.error(f"Ошибка обновления записи ID {record_id}: {e}")
            raise
    
    async def _mark_as_processed(self, pool, record_id: int, image_base64: str = None):
        """Помечает запись как обработанную (даже если изображение не сгенерировано)."""
        try:
            async with pool.acquire() as conn:
                if image_base64:
                    query = """
                    UPDATE to_publish 
                    SET 
                        "pic-base64" = $1,
                        pic = true
                    WHERE id = $2
                    """
                    await conn.execute(query, image_base64, record_id)
                else:
                    query = """
                    UPDATE to_publish 
                    SET 
                        pic = false
                    WHERE id = $1
                    """
                    await conn.execute(query, record_id)
        except Exception as e:
            logger.error(f"Ошибка пометки записи ID {record_id} как обработанной: {e}")
    
    async def cleanup(self):
        """Очистка ресурсов (если потребуется)."""
        logger.info("Очистка ресурсов Painter Service")


async def main():
    """Тестовый запуск службы."""
    painter = PainterService()
    await painter.run_monitoring()

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    asyncio.run(main())