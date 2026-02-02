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
                logger.info(f"📅 Текущая дата для очистки: {current_date}")
                
                # Сначала получаем детальную информацию о строках для удаления
                detail_query = """
                SELECT 
                    id, 
                    post_time, 
                    expire,
                    post_time + expire as expiry_date,
                    post_time + expire < CURRENT_DATE as is_expired,
                    CURRENT_DATE - (post_time + expire) as days_overdue
                FROM editor
                WHERE CURRENT_DATE > post_time + expire
                ORDER BY post_time
                """
                
                rows_to_delete = await conn.fetch(detail_query)
                
                if not rows_to_delete:
                    logger.debug("✅ В таблице editor нет устаревших записей")
                    return 0
                
                logger.info(f"🧹 Найдено {len(rows_to_delete)} строк для удаления из editor:")
                
                # Детальное логирование каждой строки
                for row in rows_to_delete:
                    logger.info(
                        f"   ❌ ID: {row['id']}, "
                        f"Дата публикации: {row['post_time']}, "
                        f"Expire: {row['expire']} дней, "
                        f"Дата истечения: {row['expiry_date']}, "
                        f"Просрочено дней: {row['days_overdue']}"
                    )
                
                # Считаем общее количество для проверки
                count_query = """
                SELECT COUNT(*) as count
                FROM editor
                WHERE CURRENT_DATE > post_time + expire
                """
                
                count_result = await conn.fetchval(count_query)
                logger.info(f"📊 Подтверждение количества для удаления: {count_result} строк")
                
                # Логируем пример расчета дат
                example_row = rows_to_delete[0]
                logger.info(f"🔍 Пример расчета для ID {example_row['id']}:")
                logger.info(f"   post_time = {example_row['post_time']}")
                logger.info(f"   expire = {example_row['expire']} дней")
                logger.info(f"   expiry_date = {example_row['post_time']} + {example_row['expire']} дней = {example_row['expiry_date']}")
                logger.info(f"   current_date = {current_date}")
                logger.info(f"   {current_date} > {example_row['expiry_date']} = {current_date > example_row['expiry_date']}")
                
                # Удаляем устаревшие строки
                logger.info(f"🗑️ Начинаем удаление {len(rows_to_delete)} строк из editor...")
                delete_query = """
                DELETE FROM editor
                WHERE CURRENT_DATE > post_time + expire
                """
                
                result = await conn.execute(delete_query)
                
                # Извлекаем количество удаленных строк из результата
                if result:
                    deleted_count = int(result.split()[1])
                else:
                    deleted_count = count_result
                
                # Проверяем, что удалилось столько же, сколько мы нашли
                if deleted_count != len(rows_to_delete):
                    logger.warning(
                        f"⚠️  Несоответствие количества: "
                        f"найдено {len(rows_to_delete)}, удалено {deleted_count}"
                    )
                
                # Логируем оставшиеся строки для отладки
                remaining_query = """
                SELECT COUNT(*) as remaining_count,
                       MIN(post_time) as earliest_post,
                       MAX(post_time) as latest_post,
                       AVG(expire) as avg_expire
                FROM editor
                """
                
                remaining_stats = await conn.fetchrow(remaining_query)
                logger.info(f"📊 Осталось в editor: {remaining_stats['remaining_count']} строк")
                logger.info(f"   Самая ранняя запись: {remaining_stats['earliest_post']}")
                logger.info(f"   Самая поздняя запись: {remaining_stats['latest_post']}")
                logger.info(f"   Средний expire: {remaining_stats['avg_expire']:.1f} дней")
                
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
                # Получаем детальную информацию о строках для удаления
                detail_query = """
                SELECT 
                    id, 
                    published,
                    published_time,
                    EXTRACT(DAY FROM NOW() - published_time) as days_ago
                FROM to_publish
                WHERE published = true
                ORDER BY published_time DESC
                """
                
                rows_to_delete = await conn.fetch(detail_query)
                
                if not rows_to_delete:
                    logger.debug("✅ В таблице to_publish нет опубликованных записей для удаления")
                    return 0
                
                logger.info(f"🧹 Найдено {len(rows_to_delete)} строк для удаления из to_publish:")
                
                # Группируем по дням для лучшего логирования
                from collections import defaultdict
                days_groups = defaultdict(int)
                
                for row in rows_to_delete:
                    days_ago = int(row['days_ago']) if row['days_ago'] else 0
                    days_groups[days_ago] += 1
                    
                    logger.info(
                        f"   ❌ ID: {row['id']}, "
                        f"Опубликовано: {row['published_time']}, "
                        f"Дней назад: {days_ago}"
                    )
                
                # Логируем статистику по дням
                logger.info("📊 Статистика по дням публикации:")
                for days_ago, count in sorted(days_groups.items()):
                    logger.info(f"   {days_ago} дней назад: {count} записей")
                
                # Удаляем опубликованные строки
                logger.info(f"🗑️ Начинаем удаление {len(rows_to_delete)} строк из to_publish...")
                delete_query = """
                DELETE FROM to_publish
                WHERE published = true
                """
                
                result = await conn.execute(delete_query)
                
                # Извлекаем количество удаленных строк из результата
                if result:
                    deleted_count = int(result.split()[1])
                else:
                    deleted_count = len(rows_to_delete)
                
                # Проверяем, что удалилось столько же, сколько мы нашли
                if deleted_count != len(rows_to_delete):
                    logger.warning(
                        f"⚠️  Несоответствие количества: "
                        f"найдено {len(rows_to_delete)}, удалено {deleted_count}"
                    )
                
                # Логируем оставшиеся строки для отладки
                remaining_query = """
                SELECT 
                    COUNT(*) as total_count,
                    COUNT(CASE WHEN published = true THEN 1 END) as published_count,
                    COUNT(CASE WHEN published = false THEN 1 END) as unpublished_count,
                    MIN(published_time) as earliest_published,
                    MAX(published_time) as latest_published
                FROM to_publish
                """
                
                remaining_stats = await conn.fetchrow(remaining_query)
                logger.info(f"📊 Статистика to_publish после очистки:")
                logger.info(f"   Всего записей: {remaining_stats['total_count']}")
                logger.info(f"   Опубликованных: {remaining_stats['published_count']}")
                logger.info(f"   Неопубликованных: {remaining_stats['unpublished_count']}")
                
                if remaining_stats['earliest_published']:
                    logger.info(f"   Самая ранняя публикация: {remaining_stats['earliest_published']}")
                if remaining_stats['latest_published']:
                    logger.info(f"   Самая поздняя публикация: {remaining_stats['latest_published']}")
                
                logger.info(f"✅ Удалено {deleted_count} строк из to_publish")
                return deleted_count
                
        except Exception as e:
            logger.error(f"❌ Ошибка очистки to_publish: {e}", exc_info=True)
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