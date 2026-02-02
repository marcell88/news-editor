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

# Конфигурация из .env (все время в UTC!)
PER_HOUR = int(os.getenv("PER_HOUR", 300))  # Символов в час
MIN_HOUR_MSK = int(os.getenv("MIN", 9))     # Минимальный час публикации по МСК (для удобства)
MAX_HOUR_MSK = int(os.getenv("MAX", 21))    # Максимальный час публикации по МСК (для удобства)
PLANNER_CHECK_INTERVAL = int(os.getenv("PLANNER_CHECK_INTERVAL", 60))

# Конвертируем в UTC сразу после загрузки (работаем только в UTC!)
MIN_HOUR_UTC = MIN_HOUR_MSK - 3  # 9:00 МСК = 6:00 UTC
MAX_HOUR_UTC = MAX_HOUR_MSK - 3  # 21:00 МСК = 18:00 UTC

class PlannerService:
    """Служба планирования публикаций"""
    
    def __init__(self):
        self.check_interval = PLANNER_CHECK_INTERVAL
        
    async def run_monitoring(self):
        """Основной цикл мониторинга"""
        try:
            logger.info("📅 Planner Service запущен")
            logger.info(f"⏰ Настройки: {MIN_HOUR_UTC}:00-{MAX_HOUR_UTC}:00 UTC "
                       f"({MIN_HOUR_MSK}:00-{MAX_HOUR_MSK}:00 МСК)")
            
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
            
            logger.info(f"🎯 Время следующей публикации: {next_unix_time} ({datetime.fromtimestamp(next_unix_time)} UTC)")
            logger.info(f"🎯 Целевой час для Timer: {target_hour}:00 UTC")
            
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
        """Рассчитывает UNIX-время следующей публикации и час для Timer (все в UTC)"""
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
                    # Если нет публикаций, используем текущее время UTC
                    now_utc = datetime.utcnow()
                    current_unix = int(now_utc.timestamp())
                    current_hour_utc = now_utc.hour
                    
                    logger.info(f"📅 Первая публикация: {current_hour_utc}:00 UTC")
                    
                    # Проверяем границы по UTC
                    if current_hour_utc > MAX_HOUR_UTC:
                        # После MAX по UTC - переносим на MIN следующего дня
                        next_unix = self._create_utc_time_days_from_now(1, MIN_HOUR_UTC)
                        target_hour = MIN_HOUR_UTC
                        logger.info(f"📅 После {MAX_HOUR_UTC}:00 UTC → переносим на {MIN_HOUR_UTC}:00 следующего дня")
                    elif current_hour_utc < MIN_HOUR_UTC:
                        # До MIN по UTC - переносим на MIN сегодня
                        next_unix = self._create_utc_time_days_from_now(0, MIN_HOUR_UTC)
                        target_hour = MIN_HOUR_UTC
                        logger.info(f"📅 До {MIN_HOUR_UTC}:00 UTC → переносим на {MIN_HOUR_UTC}:00 сегодня")
                    else:
                        # В пределах MIN-MAX по UTC - оставляем как есть
                        next_unix = current_unix
                        target_hour = current_hour_utc
                        logger.info(f"📅 В пределах {MIN_HOUR_UTC}:00-{MAX_HOUR_UTC}:00 UTC → оставляем текущее время")
                    
                    logger.info(f"📅 Результат: next_unix={next_unix} ({datetime.fromtimestamp(next_unix)} UTC), "
                               f"target_hour={target_hour}:00 UTC")
                    return next_unix, target_hour
                
                last_published = row['published']  # UNIX-время (UTC)
                length = row['length'] or 300  # Длина поста в символах
                
                # Базовый расчет времени следующей публикации
                hours_until_next = length / PER_HOUR  # length(символов) / PER_HOUR
                seconds_until_next = int(hours_until_next * 3600)
                next_unix_time = last_published + seconds_until_next
                
                # Получаем час UTC
                last_hour_utc = self._get_utc_hour_from_unix(last_published)
                next_hour_utc = self._get_utc_hour_from_unix(next_unix_time)
                
                logger.info(f"📅 Базовый расчет:")
                logger.info(f"  last_published: {last_published} ({datetime.fromtimestamp(last_published)} UTC)")
                logger.info(f"  last_hour: {last_hour_utc}:00 UTC")
                logger.info(f"  length: {length} символов, PER_HOUR: {PER_HOUR} симв/час")
                logger.info(f"  hours_until_next: {hours_until_next:.2f}ч")
                logger.info(f"  next_unix_time: {next_unix_time} ({datetime.fromtimestamp(next_unix_time)} UTC)")
                logger.info(f"  next_hour: {next_hour_utc}:00 UTC")
                
                # Упрощенная логика: просто проверяем попадание в рабочие часы
                if MIN_HOUR_UTC <= next_hour_utc <= MAX_HOUR_UTC:
                    # В рабочее время - оставляем как есть
                    final_unix_time = next_unix_time
                    logger.info(f"🎯 Время в рабочих часах ({MIN_HOUR_UTC}:00-{MAX_HOUR_UTC}:00 UTC) → оставляем")
                else:
                    # Вне рабочих часов - переносим на MIN
                    if next_hour_utc < MIN_HOUR_UTC:
                        # До MIN сегодня - переносим на MIN сегодня
                        next_datetime_utc = datetime.fromtimestamp(next_unix_time)
                        days_to_add = 0
                        logger.info(f"🎯 До {MIN_HOUR_UTC}:00 UTC → переносим на {MIN_HOUR_UTC}:00 сегодня")
                    else:
                        # После MAX - переносим на MIN следующего дня
                        next_datetime_utc = datetime.fromtimestamp(next_unix_time)
                        days_to_add = 1
                        logger.info(f"🎯 После {MAX_HOUR_UTC}:00 UTC → переносим на {MIN_HOUR_UTC}:00 следующего дня")
                    
                    # Создаем время для MIN часа
                    final_unix_time = self._create_utc_time_for_datetime(
                        next_datetime_utc, days_to_add, MIN_HOUR_UTC
                    )
                
                # Получаем финальный час UTC для Timer
                final_hour_utc = self._get_utc_hour_from_unix(final_unix_time)
                
                logger.info(f"🎯 Итог:")
                logger.info(f"  final_unix_time: {final_unix_time} ({datetime.fromtimestamp(final_unix_time)} UTC)")
                logger.info(f"  final_hour: {final_hour_utc}:00 UTC")
                logger.info(f"  target_hour: {final_hour_utc} (по UTC для Timer)")
                
                return final_unix_time, final_hour_utc
                
        except Exception as e:
            logger.error(f"❌ Ошибка при расчете времени публикации: {e}")
            # В случае ошибки используем текущее время
            now_utc = datetime.utcnow()
            current_unix = int(now_utc.timestamp())
            current_hour_utc = now_utc.hour
            return current_unix, current_hour_utc
    
    def _get_utc_hour_from_unix(self, unix_time: int) -> int:
        """Получает час UTC из UNIX-времени"""
        return (unix_time // 3600) % 24
    
    def _create_utc_time_days_from_now(self, days: int, hour_utc: int) -> int:
        """Создает UNIX-время для hour_utc:00 через N дней от сейчас в UTC"""
        # Текущее время UTC
        now_utc = datetime.utcnow()
        
        # Определяем целевую дату
        target_date = now_utc.date() + timedelta(days=days)
        
        # Создаем datetime для целевого часа
        target_datetime = datetime.combine(target_date, time(hour_utc, 0))
        
        # Возвращаем UNIX время
        return int(target_datetime.timestamp())
    
    def _create_utc_time_for_datetime(self, dt_utc: datetime, days_to_add: int, hour_utc: int) -> int:
        """Создает UNIX-время для hour_utc:00 на основе datetime UTC"""
        # Определяем целевую дату
        target_date = dt_utc.date() + timedelta(days=days_to_add)
        
        # Создаем datetime для целевого часа
        target_datetime = datetime.combine(target_date, time(hour_utc, 0))
        
        # Возвращаем UNIX время
        return int(target_datetime.timestamp())
    
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
            
            # 2. Запускаем Timer с целевым часом (target_hour уже по UTC!)
            logger.info(f"🚀 Запуск Timer Service с target_hour={target_hour}:00 UTC...")
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
                # 1. Получаем данные из editor (ДОБАВИЛ final_score)
                select_query = """
                SELECT text, mood, topic, names, author, length, final_score
                FROM editor 
                WHERE id = $1
                """
                
                editor_row = await conn.fetchrow(select_query, record_id)
                
                if not editor_row:
                    logger.error(f"❌ Запись ID {record_id} не найдена в editor")
                    return
                
                # 2. Создаем запись в to_publish (ДОБАВИЛ final_score)
                insert_query = """
                INSERT INTO to_publish 
                (text, mood, topic, names, author, length, time, final_score)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                """
                
                await conn.execute(
                    insert_query,
                    editor_row['text'],
                    editor_row['mood'],
                    editor_row['topic'],
                    editor_row['names'],
                    editor_row['author'],
                    editor_row['length'],
                    publish_time,
                    editor_row['final_score']  # ДОБАВИЛ final_score
                )
                
                publish_datetime_utc = datetime.fromtimestamp(publish_time)
                publish_hour_utc = publish_datetime_utc.hour
                
                logger.info(f"📝 Создана запись в to_publish:")
                logger.info(f"  time: {publish_time} ({publish_datetime_utc} UTC)")
                logger.info(f"  час: {publish_hour_utc}:00 UTC")
                logger.info(f"  final_score: {editor_row['final_score']}")
                
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