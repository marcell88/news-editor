# services/calculator.py
import asyncio
import logging
import os
from typing import List, Dict, Any
from dotenv import load_dotenv

from database.database import Database

logger = logging.getLogger(__name__)
load_dotenv()

# Добавим логирование загрузки .env
env_path = '.env'
logger.info(f"Пытаемся загрузить .env из: {os.path.abspath(env_path)}")
logger.info(f"Файл .env существует: {os.path.exists(env_path)}")

# Выведем все переменные из .env для отладки
if os.path.exists(env_path):
    with open(env_path, 'r') as f:
        env_content = f.read()
    logger.info(f"Содержимое .env:\n{env_content}")

# Пробуем получить каждую переменную по отдельности и логируем
lt_topic_val = os.getenv("LT_TOPIC_WEIGHT")
lt_mood_val = os.getenv("LT_MOOD_WEIGHT")
mt_topic_val = os.getenv("MT_TOPIC_WEIGHT")
mt_mood_val = os.getenv("MT_MOOD_WEIGHT")
mt_author_val = os.getenv("MT_AUTHOR_WEIGHT")
time_best_val = os.getenv("TIME_BEST_WEIGHT")
time_expire_val = os.getenv("TIME_EXPIRE_WEIGHT")

logger.info(f"LT_TOPIC_WEIGHT из env: '{lt_topic_val}' (тип: {type(lt_topic_val)})")
logger.info(f"LT_MOOD_WEIGHT из env: '{lt_mood_val}'")
logger.info(f"MT_TOPIC_WEIGHT из env: '{mt_topic_val}'")
logger.info(f"MT_MOOD_WEIGHT из env: '{mt_mood_val}'")
logger.info(f"MT_AUTHOR_WEIGHT из env: '{mt_author_val}'")
logger.info(f"TIME_BEST_WEIGHT из env: '{time_best_val}'")
logger.info(f"TIME_EXPIRE_WEIGHT из env: '{time_expire_val}'")

# Список всех переменных окружения для отладки
all_env_vars = os.environ.keys()
weight_vars = [var for var in all_env_vars if 'WEIGHT' in var.upper()]
logger.info(f"Все переменные с 'WEIGHT' в системе: {weight_vars}")

WEIGHTS = {
    "lt_topic": float(os.getenv("LT_TOPIC_WEIGHT", "0.15")),
    "lt_mood": float(os.getenv("LT_MOOD_WEIGHT", "0.15")),
    "mt_topic": float(os.getenv("MT_TOPIC_WEIGHT", "0.15")),
    "mt_mood": float(os.getenv("MT_MOOD_WEIGHT", "0.15")),
    "mt_author": float(os.getenv("MT_AUTHOR_WEIGHT", "0.15")),
    "time_best": float(os.getenv("TIME_BEST_WEIGHT", "0.20")),
    "time_expire": float(os.getenv("TIME_EXPIRE_WEIGHT", "0.05")),
}

# Логируем итоговые веса
logger.info("Итоговые веса:")
for key, value in WEIGHTS.items():
    logger.info(f"  {key}: {value}")

# Проверяем сумму весов
total_weight = sum(WEIGHTS.values())
logger.info(f"Сумма всех весов: {total_weight:.2f}")
if abs(total_weight - 1.0) > 0.001:
    logger.warning(f"Внимание! Сумма весов ({total_weight:.2f}) не равна 1.0")

class CalculatorService:
    def __init__(self):
        self.check_interval = 5
        
    async def run_monitoring(self):
        try:
            logger.info("🧮 Calculator Service запущен")
            
            while True:
                await self._check_and_calculate()
                await asyncio.sleep(self.check_interval)
                
        except Exception as e:
            logger.error(f"Ошибка в основном цикле: {e}")
    
    async def _check_and_calculate(self):
        try:
            pool = await Database.get_pool()
            
            # ТОЛЬКО ищем готовые записи для расчета
            records = await self._get_ready_records(pool)
            if records:
                logger.info(f"Найдено {len(records)} записей для расчета")
                await self._calculate_records(pool, records)
                
        except Exception as e:
            logger.error(f"Ошибка в _check_and_calculate: {e}")
    
    async def _get_ready_records(self, pool) -> List[Dict]:
        """Получаем записи, готовые для расчета"""
        try:
            async with pool.acquire() as conn:
                query = """
                SELECT id, 
                       "lt-topic", "lt-mood",
                       "mt-topic", "mt-mood", "mt-author",
                       "time-best", "time-expire"
                FROM editor 
                WHERE lt = true AND mt = true AND time = true 
                  AND analyzed = false
                ORDER BY id
                """
                rows = await conn.fetch(query)
                return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"Ошибка получения готовых записей: {e}")
            return []
    
    async def _calculate_records(self, pool, records: List[Dict]):
        """Рассчитываем оценки для записей"""
        calculated = 0
        for record in records:
            try:
                final_score = self._calculate_score(record)
                await self._update_record(pool, record['id'], final_score)
                calculated += 1
            except Exception as e:
                logger.error(f"Ошибка расчета ID {record['id']}: {e}")
        
        if calculated:
            logger.info(f"✅ Рассчитано {calculated} оценок")
    
    def _calculate_score(self, record: Dict) -> float:
        """Рассчитывает итоговую оценку"""
        try:
            # Логируем входные данные для отладки
            logger.debug(f"Расчет для ID {record['id']}:")
            for key, value in record.items():
                if key != 'id':
                    logger.debug(f"  {key}: {value} (тип: {type(value)})")
            
            # Собираем оценки
            scores = {}
            for key in WEIGHTS.keys():
                db_key = key.replace('_', '-')
                value = record.get(db_key)
                
                try:
                    if value is not None:
                        num = float(value)
                        scores[key] = num
                        logger.debug(f"  {key} -> {db_key}: {num}")
                    else:
                        scores[key] = None
                        logger.debug(f"  {key} -> {db_key}: None")
                except Exception as conv_e:
                    logger.warning(f"  Ошибка конвертации {key} ({value}): {conv_e}")
                    scores[key] = None
            
            # Разделяем валидные (>0) и невалидные
            valid = {}
            invalid_weight = 0.0
            
            for key, weight in WEIGHTS.items():
                score = scores.get(key)
                if score is not None and score > 0:
                    valid[key] = {'score': score, 'weight': weight}
                    logger.debug(f"  Валидный: {key} = {score}, вес = {weight}")
                else:
                    invalid_weight += weight
                    logger.debug(f"  Невалидный: {key}, добавляем вес {weight} к invalid_weight")
            
            logger.debug(f"  Всего валидных: {len(valid)}, invalid_weight = {invalid_weight}")
            
            if not valid:
                logger.warning(f"  Нет валидных оценок, возвращаем 5.0")
                return 5.0
            
            # Перераспределяем веса
            if invalid_weight > 0:
                weight_per_valid = invalid_weight / len(valid)
                logger.debug(f"  Перераспределяем invalid_weight {invalid_weight} на {len(valid)} валидных = {weight_per_valid} каждый")
                for data in valid.values():
                    data['weight'] += weight_per_valid
                    logger.debug(f"    Новый вес: {data['weight']}")
            
            # Рассчитываем
            total_score = 0.0
            total_weight = 0.0
            
            for key, data in valid.items():
                contribution = data['score'] * data['weight']
                total_score += contribution
                total_weight += data['weight']
                logger.debug(f"  {key}: {data['score']} * {data['weight']} = {contribution}")
            
            logger.debug(f"  Итого: total_score = {total_score}, total_weight = {total_weight}")
            
            final = total_score / total_weight if total_weight > 0 else 5.0
            final = max(1.0, min(10.0, final))
            
            logger.info(f"ID {record['id']}: итог {final:.2f}")
            return final
            
        except Exception as e:
            logger.error(f"Ошибка расчета: {e}")
            return 5.0
    
    async def _update_record(self, pool, record_id: int, final_score: float):
        """Обновляет запись с итоговой оценкой"""
        try:
            async with pool.acquire() as conn:
                query = """
                UPDATE editor 
                SET final_score = $1, analyzed = true
                WHERE id = $2
                """
                await conn.execute(query, round(final_score, 2), record_id)
        except Exception as e:
            logger.error(f"Ошибка обновления ID {record_id}: {e}")
            raise

async def main():
    calculator = CalculatorService()
    await calculator.run_monitoring()

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    asyncio.run(main())