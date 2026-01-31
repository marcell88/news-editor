# services/cleaner.py
import asyncio
import logging
from datetime import datetime, date, timedelta
from typing import List, Dict, Any
from dotenv import load_dotenv

from database.database import Database

logger = logging.getLogger(__name__)
load_dotenv()

# Константы
CHECK_INTERVAL = 3600  # Проверка каждый час (3600 секунд)

class CleanerService:
    """
    Служба для очистки базы данных
    """
    
    def __init__(self):
        logger.info("Cleaner инициализирован")
    
    async def run_monitoring(self):
        """Фоновый цикл очистки БД."""
        try:
            logger.info("🚀 Cleaner Service запущен")
            
            while True:
                await self._clean_database()
                await asyncio.sleep(CHECK_INTERVAL)
                
        except asyncio.CancelledError:
            logger.info("Cleaner Service остановлен")
        except Exception as e:
            logger.error(f"Ошибка в Cleaner: {e}")
            await asyncio.sleep(60)
    
    async def _clean_database(self):
        """Выполняет очистку всех таблиц."""
        try:
            pool = await Database.get_pool()
            
            # Очищаем таблицу editor
            editor_deleted = await self._clean_editor_table(pool)
            
            # Очищаем таблицу to_publish
            to_publish_deleted = await self._clean_to_publish_table(pool)
            
            if editor_deleted or to_publish_deleted:
                logger.info(f"🗑️ Очищено: editor={editor_deleted}, to_publish={to_publish_deleted}")
            else:
                logger.debug("🗑️ Нечего очищать")
            
        except Exception as e:
            logger.error(f"Ошибка в _clean_database: {e}")
    
    async def _clean_editor_table(self, pool) -> int:
        """
        Очищает таблицу editor.
        Удаляет строки, где текущая дата > post_time + expire дней.
        post_time - тип date (например, 2026-01-31)
        expire - integer (количество дней)
        """
        try:
            async with pool.acquire() as conn:
                # Получаем текущую дату
                current_date = date.today()
                
                # Сначала считаем количество строк для удаления (для логирования)
                count_query = """
                SELECT COUNT(*) as count
                FROM editor
                WHERE CURRENT_DATE > post_time + expire
                """
                
                count_result = await conn.fetchval(count_query)
                
                if count_result == 0:
                    return 0
                
                logger.info(f"🧹 Найдено {count_result} строк для удаления из editor")
                logger.info(f"📅 Текущая дата: {current_date}")
                
                # Удаляем устаревшие строки
                delete_query = """
                DELETE FROM editor
                WHERE CURRENT_DATE > post_time + expire
                """
                
                result = await conn.execute(delete_query)
                
                # Извлекаем количество удаленных строк из результата
                # Формат результата: "DELETE count"
                if result:
                    deleted_count = int(result.split()[1])
                else:
                    deleted_count = count_result
                
                logger.info(f"✅ Удалено {deleted_count} строк из editor")
                return deleted_count
                
        except Exception as e:
            logger.error(f"❌ Ошибка очистки editor: {e}")
            return 0
    
    async def _clean_to_publish_table(self, pool) -> int:
        """
        Очищает таблицу to_publish.
        Удаляет строки, где published = true.
        """
        try:
            async with pool.acquire() as conn:
                # Сначала считаем количество строк для удаления (для логирования)
                count_query = """
                SELECT COUNT(*) as count
                FROM to_publish
                WHERE published = true
                """
                
                count_result = await conn.fetchval(count_query)
                
                if count_result == 0:
                    return 0
                
                logger.info(f"🧹 Найдено {count_result} строк для удаления из to_publish")
                
                # Удаляем опубликованные строки
                delete_query = """
                DELETE FROM to_publish
                WHERE published = true
                """
                
                result = await conn.execute(delete_query)
                
                # Извлекаем количество удаленных строк из результата
                if result:
                    deleted_count = int(result.split()[1])
                else:
                    deleted_count = count_result
                
                logger.info(f"✅ Удалено {deleted_count} строк из to_publish")
                return deleted_count
                
        except Exception as e:
            logger.error(f"❌ Ошибка очистки to_publish: {e}")
            return 0


async def main():
    """Запуск службы."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    cleaner = CleanerService()
    
    try:
        await Database.initialize_database()
        logger.info("✅ База данных готова")
    except Exception as e:
        logger.error(f"❌ Ошибка БД: {e}")
        return
    
    await cleaner.run_monitoring()

if __name__ == "__main__":
    asyncio.run(main())