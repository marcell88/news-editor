# services/timer.py
import asyncio
import logging
import os
import math
from typing import List, Dict, Any
from datetime import datetime, date, timedelta
from dotenv import load_dotenv

from database.database import Database

logger = logging.getLogger(__name__)

# Загрузка переменных окружения
load_dotenv()

# ========== КОНСТАНТЫ ДЛЯ ШТРАФОВ ЗА ЭНТРОПИЮ ==========
ENTROPY_PENALTY_MAP = {
    0: 0,  # Первый - без штрафа
    1: 0,  # Второй - без штрафа
    2: 1,  # Третий - штраф 1
    3: 1,  # Четвертый - штраф 1
    4: 2,  # Пятый - штраф 2
    5: 2,  # Шестой - штраф 2
}
DEFAULT_ENTROPY_PENALTY = 3
# ========================================================

class TimerService:
    """Служба для оценки времени публикации и срочности новостей"""
    
    def __init__(self, target_hour: int = None, target_date: date = None):
        """
        Инициализация TimerService
        
        Args:
            target_hour: Целевой час для оценки (0-23). Если None - берется текущий час.
            target_date: Целевая дата для оценки. Если None - берется текущая дата.
        """
        self.current_date = target_date or date.today()
        self.current_hour = target_hour or datetime.now().hour
        
        # Проверяем что час в допустимом диапазоне
        if not (0 <= self.current_hour <= 23):
            raise ValueError(f"Некорректный час: {self.current_hour}. Должен быть 0-23")
        
        logger.info(f"⏰ Инициализация TimerService: дата={self.current_date}, час={self.current_hour}")
        
    async def run_analysis(self):
        """Основной метод анализа времени - работает ТОЛЬКО с time=false"""
        try:
            logger.info("⏰ Запуск Timer Service...")
            logger.info(f"📅 Целевая дата: {self.current_date}")
            logger.info(f"🕐 Целевой час: {self.current_hour}")
            
            # 1. Подключаемся к БД
            pool = await Database.get_pool()
            
            # 2. Получаем ТОЛЬКО записи с time = false (предполагается, что ВСЕ записи сброшены на false извне)
            editor_records = await self._get_editor_records_with_time_false(pool)
            
            if not editor_records:
                logger.info("✅ В таблице editor нет записей с time = false для обработки")
                return
            
            logger.info(f"🔍 Найдено {len(editor_records)} записей с time = false для обработки")
            
            # 3. Получаем статистику по best_times для ВСЕХ записей
            # (предполагаем, что все записи имеют time=false, так как сброшены извне)
            all_records_stats = await self._get_all_records_best_times(pool)
            
            # 4. Рассчитываем текущее покрытие часов для ВСЕХ записей
            coverage = self._calculate_coverage(editor_records)  # Считаем только по обрабатываемым записям
            
            # 5. Рассчитываем исходную энтропию
            original_entropy = self._calculate_entropy(coverage)
            logger.info(f"📊 Исходная энтропия покрытия: {original_entropy:.4f}")
            
            # 6. Рассчитываем штрафы за энтропию для обрабатываемых записей
            # Предполагаем, что каждая запись добавляется к пустому распределению (все time=false)
            entropy_penalties = await self._calculate_entropy_penalties_for_records(
                editor_records, original_entropy, coverage
            )
            
            # 7. Обрабатываем каждую запись с time = false
            processed_count = 0
            for record in editor_records:
                try:
                    await self._process_single_record(
                        pool, record, all_records_stats, 
                        entropy_penalties.get(record['id'], DEFAULT_ENTROPY_PENALTY)
                    )
                    processed_count += 1
                    
                except Exception as e:
                    logger.error(f"❌ Ошибка при обработке записи ID {record['id']}: {e}")
            
            logger.info(f"✅ Обработано {processed_count} записей Timer Service")
            
        except Exception as e:
            logger.error(f"❌ Ошибка в Timer Service: {e}")
            raise
    
    async def _get_editor_records_with_time_false(self, pool) -> List[Dict]:
        """Получает записи из таблицы editor с time = false"""
        try:
            async with pool.acquire() as conn:
                query = """
                SELECT id, post_time, expire, best_times 
                FROM editor 
                WHERE time = false
                ORDER BY id
                """
                
                rows = await conn.fetch(query)
                return [dict(row) for row in rows]
                
        except Exception as e:
            logger.error(f"Ошибка при получении записей editor с time=false: {e}")
            return []
    
    async def _get_all_records_best_times(self, pool) -> Dict[int, float]:
        """Получает статистику по best_times для ВСЕХ записей editor"""
        try:
            async with pool.acquire() as conn:
                query = """
                SELECT best_times 
                FROM editor 
                WHERE best_times IS NOT NULL 
                  AND best_times != '{}'
                """
                
                rows = await conn.fetch(query)
                
                # Считаем сколько раз каждый час встречается в best_times
                hour_counts = {hour: 0 for hour in range(24)}
                
                for row in rows:
                    if row['best_times']:
                        for hour in row['best_times']:
                            if 0 <= hour <= 23:
                                hour_counts[hour] += 1
                
                # Преобразуем в редкость (чем больше записей используют час, тем меньше его "ценность")
                total_records = len(rows)
                if total_records > 0:
                    hour_rarity = {}
                    for hour, count in hour_counts.items():
                        frequency = count / total_records
                        rarity = 1 - frequency  # Редкость = 1 - частота
                        hour_rarity[hour] = rarity
                else:
                    hour_rarity = {hour: 1.0 for hour in range(24)}  # Все часы одинаково редки
                
                logger.info(f"📊 Статистика часов: обработано {total_records} записей с best_times")
                
                # Логируем топ-5 самых редких часов
                rare_hours = sorted(hour_rarity.items(), key=lambda x: x[1], reverse=True)[:5]
                logger.info("🏆 Самые редкие часы для публикации:")
                for hour, rarity in rare_hours:
                    logger.info(f"  Час {hour:02d}:00 - редкость {rarity:.3f}")
                
                return hour_rarity
                
        except Exception as e:
            logger.error(f"Ошибка при получении статистики часов: {e}")
            return {hour: 1.0 for hour in range(24)}
    
    def _calculate_coverage(self, records: List[Dict]) -> List[float]:
        """Рассчитывает покрытие для всех часов (0-23) на основе best_times"""
        coverage = [0.0] * 24
        
        for record in records:
            best_times = record.get('best_times', [])
            if not best_times:
                continue
                
            for hour in range(24):
                # Находим минимальное расстояние до любого best_time
                min_distance = float('inf')
                for best_hour in best_times:
                    if 0 <= best_hour <= 23:
                        # Расстояние с учетом круговой природы времени
                        distance = min(
                            abs(hour - best_hour),
                            24 - abs(hour - best_hour)
                        )
                        if distance < min_distance:
                            min_distance = distance
                
                # ЛИНЕЙНОЕ покрытие: 10 - distance, но не менее 0
                contribution = max(0, 10 - min_distance)
                coverage[hour] += contribution
        
        return coverage
    
    def _calculate_entropy(self, coverage: List[float]) -> float:
        """Рассчитывает энтропию Шеннона для распределения покрытия"""
        total = sum(coverage)
        if total <= 0:
            return 0.0
        
        entropy = 0.0
        for value in coverage:
            if value > 0:
                p = value / total
                entropy -= p * math.log2(p)
        
        return entropy
    
    def _calculate_record_contribution(self, best_times: List[int]) -> List[float]:
        """Рассчитывает вклад одной записи в покрытие каждого часа"""
        contribution = [0.0] * 24
        
        if not best_times:
            return contribution
            
        for hour in range(24):
            # Находим минимальное расстояние до любого best_time
            min_distance = float('inf')
            for best_hour in best_times:
                if 0 <= best_hour <= 23:
                    # Расстояние с учетом круговой природы времени
                    distance = min(
                        abs(hour - best_hour),
                        24 - abs(hour - best_hour)
                    )
                    if distance < min_distance:
                        min_distance = distance
            
            # ЛИНЕЙНОЕ покрытие: 10 - distance, но не менее 0
            contribution[hour] = max(0, 10 - min_distance)
        
        return contribution
    
    async def _calculate_entropy_penalties_for_records(self, records: List[Dict], 
                                                     original_entropy: float,
                                                     original_coverage: List[float]) -> Dict[int, int]:
        """
        Рассчитывает штрафы за энтропию для обрабатываемых записей
        Предполагается, что все записи имеют time=false и добавляются к пустому распределению
        """
        if not records:
            return {}
        
        entropy_changes = []
        
        for record in records:
            record_id = record['id']
            best_times = record.get('best_times', [])
            
            if not best_times:
                # Если нет best_times - худший случай (не вносит разнообразия)
                entropy_changes.append((record_id, -float('inf')))
                continue
            
            # Рассчитываем вклад этой записи
            contribution = self._calculate_record_contribution(best_times)
            
            # Новое покрытие С учетом этой записи
            # Предполагаем, что запись добавляется к существующему распределению
            new_coverage = [original_coverage[h] + contribution[h] for h in range(24)]
            
            # Новая энтропия с учетом этой записи
            new_entropy = self._calculate_entropy(new_coverage)
            
            # Изменение энтропии (положительное = увеличивает разнообразие)
            delta_entropy = new_entropy - original_entropy
            
            entropy_changes.append((record_id, delta_entropy))
        
        # Сортируем по изменению энтропии (от наибольшего увеличения к наименьшему)
        # Чем больше увеличивает энтропию (разнообразие), тем лучше
        entropy_changes.sort(key=lambda x: x[1], reverse=True)
        
        # Назначаем штрафы по рангу согласно ENTROPY_PENALTY_MAP
        penalties = {}
        for rank, (record_id, delta_entropy) in enumerate(entropy_changes):
            penalty = ENTROPY_PENALTY_MAP.get(rank, DEFAULT_ENTROPY_PENALTY)
            penalties[record_id] = penalty
            
            logger.debug(f"  Запись {record_id}: ΔH={delta_entropy:.4f}, rank={rank}, penalty={penalty}")
        
        # Логируем топ-5 записей
        if entropy_changes:
            logger.info("🏆 Топ-5 записей по влиянию на энтропию (чем выше ΔH, тем лучше):")
            for rank, (record_id, delta_entropy) in enumerate(entropy_changes[:5]):
                penalty = penalties[record_id]
                logger.info(f"  Ранг {rank}: ID={record_id}, ΔH={delta_entropy:.4f}, штраф={penalty}")
        
        return penalties
    
    async def _process_single_record(self, pool, record: Dict, hour_rarity: Dict[int, float],
                                   entropy_penalty: int):
        """Обрабатывает одну запись editor с time = false"""
        record_id = record['id']
        post_time = record['post_time']
        expire_days = record['expire'] or 3
        best_times = record['best_times'] or []
        
        # 1. Рассчитываем time-expire оценку
        time_expire_score = self._calculate_expire_score(post_time, expire_days)
        
        # 2. Рассчитываем базовую time-best оценку (линейно)
        base_time_score = self._calculate_base_best_time_score(best_times, hour_rarity)
        
        # 3. Финальная оценка time-best (вычитаем штраф, но не меньше 1)
        time_best_score = max(1, base_time_score - entropy_penalty)
        
        # 4. Обновляем запись в БД - устанавливаем time = true
        await self._update_editor_record_time(pool, record_id, time_best_score, time_expire_score)
        
        # Детальное логирование
        logger.info(f"✅ Запись {record_id}: best_times={best_times}, "
                   f"base={base_time_score}, entropy_penalty={entropy_penalty}, "
                   f"final={time_best_score}, expire={time_expire_score}")
    
    def _calculate_expire_score(self, post_date: date, expire_days: int) -> int:
        """Рассчитывает оценку срочности от 1 до 10"""
        try:
            expire_date = post_date + timedelta(days=expire_days)
            
            if self.current_date >= expire_date:
                return 10
            
            total_life = expire_days
            days_passed = (self.current_date - post_date).days
            
            if total_life > 0:
                percentage_passed = (days_passed / total_life) * 100
            else:
                percentage_passed = 100
            
            if percentage_passed >= 90:
                return 10
            elif percentage_passed >= 80:
                return 9
            elif percentage_passed >= 70:
                return 8
            elif percentage_passed >= 60:
                return 7
            elif percentage_passed >= 50:
                return 6
            elif percentage_passed >= 40:
                return 5
            elif percentage_passed >= 30:
                return 4
            elif percentage_passed >= 20:
                return 3
            elif percentage_passed >= 10:
                return 2
            else:
                return 1
                
        except Exception as e:
            logger.error(f"Ошибка расчета expire_score: {e}")
            return 5
    
    def _calculate_base_best_time_score(self, best_times: List[int], hour_rarity: Dict[int, float]) -> int:
        """Рассчитывает базовую оценку времени публикации (ЛИНЕЙНО) от 1 до 10"""
        try:
            if not best_times:
                return 5
            
            logger.debug(f"  Запись: target_hour={self.current_hour}, best_times={best_times}")
            
            # Находим минимальное расстояние от ЦЕЛЕВОГО часа до любого best_time
            min_distance = float('inf')
            best_hour_for_score = None
            
            for best_hour in best_times:
                if 0 <= best_hour <= 23:
                    # Расстояние с учетом круговой природы времени
                    distance = min(
                        abs(self.current_hour - best_hour),
                        24 - abs(self.current_hour - best_hour)
                    )
                    
                    logger.debug(f"    Час {best_hour}: distance={distance}")
                    
                    if distance < min_distance:
                        min_distance = distance
                        best_hour_for_score = best_hour
            
            if best_hour_for_score is None:
                logger.debug(f"    Нет валидных часов, возвращаем 5")
                return 5
            
            # ЛИНЕЙНАЯ оценка: 10 - distance, но не менее 1
            base_score = max(1, 10 - min_distance)
            
            # Учитываем редкость часа
            rarity = hour_rarity.get(best_hour_for_score, 1.0)
            
            # Редкость добавляет от 0 до 3 баллов
            rarity_bonus = rarity * 3
            
            final_score = base_score + rarity_bonus
            
            # Ограничиваем от 1 до 10
            final_score = max(1, min(10, final_score))
            
            logger.debug(f"    Результат: distance={min_distance}, base={base_score}, "
                        f"rarity={rarity:.2f}, bonus={rarity_bonus:.1f}, final={int(round(final_score))}")
            
            return int(round(final_score))
            
        except Exception as e:
            logger.error(f"Ошибка расчета base_best_time_score: {e}")
            return 5
    
    async def _update_editor_record_time(self, pool, record_id: int, time_best_score: int, time_expire_score: int):
        """Обновляет запись в таблице editor с time-оценками и устанавливает time = true"""
        try:
            async with pool.acquire() as conn:
                update_query = """
                UPDATE editor 
                SET "time-best" = $1, 
                    "time-expire" = $2,
                    time = true
                WHERE id = $3
                """
                
                await conn.execute(update_query, time_best_score, time_expire_score, record_id)
                
        except Exception as e:
            logger.error(f"Ошибка при обновлении записи editor ID {record_id}: {e}")
            raise

async def main():
    """Основная функция службы (для тестирования)"""
    import sys
    if len(sys.argv) > 1:
        try:
            target_hour = int(sys.argv[1])
            timer = TimerService(target_hour=target_hour)
        except ValueError:
            logger.error(f"Некорректный час: {sys.argv[1]}")
            return
    else:
        timer = TimerService()
    
    await timer.run_analysis()

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    asyncio.run(main())