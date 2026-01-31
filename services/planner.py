# services/planner.py
import asyncio
import logging
import os
import math
from datetime import datetime, time, timedelta
from typing import Dict, Any, Optional, Tuple
from dotenv import load_dotenv

from database.database import Database
from services.mt_balancer import MTBalancer
from services.timer import TimerService
from services.calculator import CalculatorService

logger = logging.getLogger(__name__)

# Загрузка переменных окружения
load_dotenv()

# Конфигурация из .env
PER_HOUR = int(os.getenv("PER_HOUR", 300))  # Символов в час
MIN_HOUR = int(os.getenv("MIN", 9))         # Минимальный час публикации
MAX_HOUR = int(os.getenv("MAX", 21))        # Максимальный час публикации
PLANNER_CHECK_INTERVAL = int(os.getenv("PLANNER_CHECK_INTERVAL", 60))  # Проверка каждые 60 сек

class PlannerService:
    """Служба планирования публикаций"""
    
    def __init__(self):
        self.check_interval = PLANNER_CHECK_INTERVAL
        
    async def run_monitoring(self):
        """Основной цикл мониторинга"""
        try:
            logger.info("📅 Planner Service запущен")
            
            while True:
                await self._check_and_plan()
                await asyncio.sleep(self.check_interval)
                
        except Exception as e:
            logger.error(f"❌ Ошибка в Planner Service: {e}")
    
    async def _check_and_plan(self):
        """Проверяет необходимость планирования и выполняет его"""
        try:
            pool = await Database.get_pool()
            
            # 1. Проверяем, нужно ли запускать планирование
            should_run = await self._should_run_planning(pool)
            
            if not should_run:
                logger.debug("⏳ Planner: next = true, ждем следующей проверки")
                return
            
            logger.info("🚀 Planner: запуск процесса планирования...")
            
            # 2. Рассчитываем время следующей публикации и целевой час для Timer
            next_unix_time, target_hour = await self._calculate_next_publish_time_and_hour(pool)
            
            logger.info(f"🎯 Время следующей публикации: {next_unix_time} ({datetime.fromtimestamp(next_unix_time)})")
            logger.info(f"🎯 Целевой час для Timer: {target_hour}")
            
            # 3. Сбрасываем флаги в editor
            await self._reset_editor_flags(pool)
            
            # 4. Запускаем MT Balancer и Timer
            await self._run_services(target_hour)
            
            # 5. Ждем завершения Calculator (фоновый, но нужно дождаться расчетов)
            await asyncio.sleep(30)  # Ждем 30 секунд для расчетов
            
            # 6. Выбираем лучшую запись
            best_record_id = await self._select_best_record(pool)
            
            if not best_record_id:
                logger.warning("⚠️ Не найдена лучшая запись для публикации")
                return
            
            # 7. Удаляем запись из editor и создаем в to_publish с рассчитанным временем
            await self._move_to_publish(pool, best_record_id, next_unix_time)
            
            # 8. Устанавливаем next = true
            await self._set_next_true(pool)
            
            logger.info("✅ Planner: процесс планирования завершен")
            
        except Exception as e:
            logger.error(f"❌ Ошибка в процессе планирования: {e}")
    
    async def _should_run_planning(self, pool) -> bool:
        """Проверяет, нужно ли запускать планирование"""
        try:
            async with pool.acquire() as conn:
                # Получаем запись с максимальным id в published
                query = """
                SELECT next 
                FROM published 
                WHERE id = (SELECT MAX(id) FROM published)
                """
                
                result = await conn.fetchval(query)
                
                if result is None:
                    # Если таблица published пуста, запускаем планирование
                    logger.info("📋 Таблица published пуста, запускаем первое планирование")
                    return True
                
                # Запускаем если next = false
                return not result
                
        except Exception as e:
            logger.error(f"❌ Ошибка при проверке next флага: {e}")
            return False
    
    async def _calculate_next_publish_time_and_hour(self, pool) -> Tuple[int, int]:
        """Рассчитывает UNIX-время следующей публикации и час для Timer"""
        try:
            async with pool.acquire() as conn:
                # Получаем последнюю публикацию
                query = """
                SELECT published, length 
                FROM published 
                WHERE id = (SELECT MAX(id) FROM published)
                """
                
                row = await conn.fetchrow(query)
                
                if not row:
                    # Если нет публикаций, используем текущее время
                    now = datetime.now()
                    current_unix = int(now.timestamp())
                    current_hour = now.hour
                    
                    # Проверяем границы для первого поста
                    if current_hour > MAX_HOUR:
                        # После MAX - переносим на MIN следующего дня
                        next_morning = datetime.combine(
                            now.date() + timedelta(days=1),
                            time(MIN_HOUR, 0)
                        )
                        next_unix = int(next_morning.timestamp())
                        target_hour = MIN_HOUR
                    elif current_hour < MIN_HOUR:
                        # До MIN - переносим на MIN сегодня
                        this_morning = datetime.combine(
                            now.date(),
                            time(MIN_HOUR, 0)
                        )
                        next_unix = int(this_morning.timestamp())
                        target_hour = MIN_HOUR
                    else:
                        # В пределах MIN-MAX - оставляем как есть
                        next_unix = current_unix
                        target_hour = current_hour
                    
                    logger.info(f"📅 Первая публикация: hour={current_hour}, next_unix={next_unix}, target_hour={target_hour}")
                    return next_unix, target_hour
                
                last_published = row['published']  # UNIX-время
                length = row['length'] or 300  # Длина поста в символах
                
                # Базовый расчет времени следующей публикации
                hours_until_next = length / PER_HOUR  # length(символов) / PER_HOUR
                seconds_until_next = int(hours_until_next * 3600)
                next_unix_time = last_published + seconds_until_next
                
                logger.info(f"📅 Базовый расчет:")
                logger.info(f"  last_published: {last_published} ({datetime.fromtimestamp(last_published)})")
                logger.info(f"  length: {length} символов, PER_HOUR: {PER_HOUR} симв/час")
                logger.info(f"  hours_until_next: {hours_until_next:.2f}ч, seconds_until_next: {seconds_until_next}с")
                logger.info(f"  next_unix_time: {next_unix_time} ({datetime.fromtimestamp(next_unix_time)})")
                
                # Определяем окно относительно последней публикации
                last_datetime = datetime.fromtimestamp(last_published)
                window_start, window_end = self._get_window_for_datetime(last_datetime)
                
                logger.info(f"📅 Окно для последней публикации ({last_datetime}):")
                logger.info(f"  window_start: {window_start}")
                logger.info(f"  window_end: {window_end}")
                
                # Проверяем где была последняя публикация
                last_was_in_window = window_start <= last_datetime <= window_end
                next_datetime = datetime.fromtimestamp(next_unix_time)
                next_in_window = window_start <= next_datetime <= window_end
                
                logger.info(f"📅 Положение публикаций:")
                logger.info(f"  last_was_in_window: {last_was_in_window}")
                logger.info(f"  next_in_window: {next_in_window}")
                
                # Применяем правила
                final_unix_time = next_unix_time
                
                if last_was_in_window and next_in_window:
                    # Обе в окне → переносим на MIN следующего дня
                    publish_date = window_end.date()
                    publish_time = datetime.combine(publish_date, time(MIN_HOUR, 0))
                    final_unix_time = int(publish_time.timestamp())
                    logger.info(f"🎯 Правило 1: обе в окне → переносим на {publish_time}")
                    
                elif not last_was_in_window and next_in_window:
                    # Разовый ночной залет → оставляем как есть
                    logger.info(f"🎯 Правило 2: разовый ночной залет → оставляем {next_datetime}")
                    # final_unix_time уже = next_unix_time
                    
                elif next_datetime.hour < MIN_HOUR and not next_in_window:
                    # Ранняя пташка (до MIN но не в окне) → переносим на MIN того же дня
                    publish_date = next_datetime.date()
                    publish_time = datetime.combine(publish_date, time(MIN_HOUR, 0))
                    final_unix_time = int(publish_time.timestamp())
                    logger.info(f"🎯 Правило 3: ранняя пташка → переносим на {publish_time}")
                    
                else:
                    # Все остальное → оставляем как есть
                    logger.info(f"🎯 Правило 4: все остальное → оставляем {next_datetime}")
                    # final_unix_time уже = next_unix_time
                
                # Из финального UNIX-времени получаем час для Timer
                final_datetime = datetime.fromtimestamp(final_unix_time)
                target_hour = final_datetime.hour
                
                logger.info(f"🎯 Итог: final_unix_time={final_unix_time} ({final_datetime}), target_hour={target_hour}")
                
                return final_unix_time, target_hour
                
        except Exception as e:
            logger.error(f"❌ Ошибка при расчете времени публикации: {e}")
            # В случае ошибки используем текущее время
            now = datetime.now()
            current_unix = int(now.timestamp())
            current_hour = now.hour
            return current_unix, current_hour
    
    def _get_window_for_datetime(self, dt: datetime) -> Tuple[datetime, datetime]:
        """Определяет окно (MAX-следующий MIN) для заданного datetime"""
        if dt.hour < MIN_HOUR:
            # Время в "закрытом окне" (ночью/рано утром)
            # Окно: предыдущий день MAX - сегодня MIN
            window_start = datetime.combine(
                dt.date() - timedelta(days=1),
                time(MAX_HOUR, 0)
            )
            window_end = datetime.combine(
                dt.date(),
                time(MIN_HOUR, 0)
            )
        else:
            # Время в "дневное время"
            # Окно: сегодня MAX - завтра MIN
            window_start = datetime.combine(
                dt.date(),
                time(MAX_HOUR, 0)
            )
            window_end = datetime.combine(
                dt.date() + timedelta(days=1),
                time(MIN_HOUR, 0)
            )
        
        return window_start, window_end
    
    async def _reset_editor_flags(self, pool):
        """Сбрасывает флаги mt, time, analyzed в таблице editor"""
        try:
            async with pool.acquire() as conn:
                update_query = """
                UPDATE editor 
                SET mt = false, 
                    time = false, 
                    analyzed = false
                """
                
                result = await conn.execute(update_query)
                logger.info(f"🔄 Сброшены флаги mt, time, analyzed во всех записях editor")
                
        except Exception as e:
            logger.error(f"❌ Ошибка при сбросе флагов editor: {e}")
            raise
    
    async def _run_services(self, target_hour: int):
        """Запускает MT Balancer и Timer"""
        try:
            # 1. Запускаем MT Balancer
            logger.info("🚀 Запуск MT Balancer...")
            mt_balancer = MTBalancer()
            await mt_balancer.run_analysis()
            logger.info("✅ MT Balancer завершен")
            
            # Небольшая пауза перед Timer
            await asyncio.sleep(5)
            
            # 2. Запускаем Timer с целевым часом
            logger.info(f"🚀 Запуск Timer Service с target_hour={target_hour}...")
            timer_service = TimerService(target_hour=target_hour)
            await timer_service.run_analysis()
            logger.info("✅ Timer Service завершен")
            
            # 3. Calculator работает в фоне, он сам подхватит готовые записи
            
        except Exception as e:
            logger.error(f"❌ Ошибка при запуске сервисов: {e}")
            raise
    
    async def _select_best_record(self, pool) -> Optional[int]:
        """Выбирает лучшую запись для публикации"""
        try:
            async with pool.acquire() as conn:
                # Ищем записи с максимальным final_score
                query = """
                SELECT id, final_score, "time-best", "time-expire"
                FROM editor 
                WHERE analyzed = true 
                  AND final_score IS NOT NULL
                ORDER BY final_score DESC, 
                         "time-best" DESC, 
                         "time-expire" DESC,
                         id ASC
                LIMIT 1
                """
                
                row = await conn.fetchrow(query)
                
                if not row:
                    logger.warning("⚠️ Нет записей с final_score для выбора")
                    return None
                
                record_id = row['id']
                logger.info(f"🏆 Выбрана лучшая запись ID {record_id}: "
                           f"final_score={row['final_score']}, "
                           f"time-best={row['time-best']}, "
                           f"time-expire={row['time-expire']}")
                
                return record_id
                
        except Exception as e:
            logger.error(f"❌ Ошибка при выборе лучшей записи: {e}")
            return None
    
    async def _move_to_publish(self, pool, record_id: int, publish_time: int):
        """Перемещает запись из editor в to_publish с заданным временем"""
        try:
            async with pool.acquire() as conn:
                # 1. Получаем данные из editor
                select_query = """
                SELECT text, mood, topic, names, author, length
                FROM editor 
                WHERE id = $1
                """
                
                editor_row = await conn.fetchrow(select_query, record_id)
                
                if not editor_row:
                    logger.error(f"❌ Запись ID {record_id} не найдена в editor")
                    return
                
                # 2. Создаем запись в to_publish
                insert_query = """
                INSERT INTO to_publish 
                (text, mood, topic, names, author, length, time)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                """
                
                await conn.execute(
                    insert_query,
                    editor_row['text'],
                    editor_row['mood'],
                    editor_row['topic'],
                    editor_row['names'],
                    editor_row['author'],
                    editor_row['length'],
                    publish_time
                )
                
                publish_datetime = datetime.fromtimestamp(publish_time)
                logger.info(f"📝 Создана запись в to_publish: time={publish_time} ({publish_datetime})")
                
                # 3. Удаляем запись из editor
                delete_query = "DELETE FROM editor WHERE id = $1"
                await conn.execute(delete_query, record_id)
                
                logger.info(f"🗑️ Удалена запись ID {record_id} из editor")
                
        except Exception as e:
            logger.error(f"❌ Ошибка при перемещении записи в to_publish: {e}")
            raise
    
    async def _set_next_true(self, pool):
        """Устанавливает next = true для последней записи в published"""
        try:
            async with pool.acquire() as conn:
                update_query = """
                UPDATE published 
                SET next = true
                WHERE id = (SELECT MAX(id) FROM published)
                """
                
                result = await conn.execute(update_query)
                logger.info("✅ Установлен next = true для последней записи published")
                
        except Exception as e:
            logger.error(f"❌ Ошибка при установке next = true: {e}")

async def main():
    """Основная функция службы"""
    planner = PlannerService()
    await planner.run_monitoring()

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    asyncio.run(main())