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
        Удаляет строки, где текущая дата приложения > post_time + expire дней.
        """
        try:
            async with pool.acquire() as conn:
                # Используем дату приложения, а не из БД!
                current_date = date.today()
                logger.info(f"📅 Текущая дата приложения: {current_date}")
                
                # Сначала получаем записи для удаления с деталями
                select_query = """
                SELECT 
                    id, 
                    post_time, 
                    expire,
                    post_time + expire as expiry_date
                FROM editor
                WHERE post_time + expire < $1::date
                ORDER BY post_time
                """
                
                rows_to_delete = await conn.fetch(select_query, current_date)
                
                if not rows_to_delete:
                    logger.debug("✅ В таблице editor нет устаревших записей")
                    return 0
                
                logger.info(f"🧹 Найдено {len(rows_to_delete)} строк для удаления из editor:")
                
                for row in rows_to_delete:
                    days_overdue = (current_date - row['expiry_date']).days
                    logger.info(
                        f"   ❌ ID: {row['id']}, "
                        f"Дата публикации: {row['post_time']}, "
                        f"Expire: {row['expire']} дней, "
                        f"Дата истечения: {row['expiry_date']}, "
                        f"Просрочено дней: {days_overdue}"
                    )
                
                # Удаляем одним запросом с использованием даты приложения
                delete_query = """
                DELETE FROM editor
                WHERE post_time + expire < $1::date
                """
                
                result = await conn.execute(delete_query, current_date)
                
                if result:
                    deleted_count = int(result.split()[1])
                else:
                    deleted_count = len(rows_to_delete)
                
                logger.info(f"✅ Удалено {deleted_count} строк из editor")
                return deleted_count
                
        except Exception as e:
            logger.error(f"❌ Ошибка очистки editor: {e}", exc_info=True)
            return 0
    
    async def _clean_to_publish_table(self, pool) -> int:
        """
        Очищает таблицу to_publish.
        Удаляет строки, где published = true.
        """
        try:
            async with pool.acquire() as conn:
                # Сначала считаем сколько удалим
                count_query = """
                SELECT COUNT(*) FROM to_publish WHERE published = true
                """
                count = await conn.fetchval(count_query)
                
                if count == 0:
                    logger.debug("✅ В таблице to_publish нет опубликованных записей для удаления")
                    return 0
                
                logger.info(f"🧹 Найдено {count} строк для удаления из to_publish")
                
                # Удаляем
                delete_query = """
                DELETE FROM to_publish WHERE published = true
                """
                
                result = await conn.execute(delete_query)
                
                if result:
                    deleted_count = int(result.split()[1])
                else:
                    deleted_count = count
                
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