# services/calculator.py
import asyncio
import logging
import os
from typing import List, Dict, Any
from dotenv import load_dotenv

from database.database import Database

logger = logging.getLogger(__name__)
load_dotenv()

WEIGHTS = {
    "lt_topic": float(os.getenv("LT_TOPIC_WEIGHT", "0.15")),
    "lt_mood": float(os.getenv("LT_MOOD_WEIGHT", "0.15")),
    "mt_topic": float(os.getenv("MT_TOPIC_WEIGHT", "0.15")),
    "mt_mood": float(os.getenv("MT_MOOD_WEIGHT", "0.15")),
    "mt_author": float(os.getenv("MT_AUTHOR_WEIGHT", "0.15")),
    "time_best": float(os.getenv("TIME_BEST_WEIGHT", "0.20")),
    "time_expire": float(os.getenv("TIME_EXPIRE_WEIGHT", "0.05")),
}

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
            # Собираем оценки
            scores = {}
            for key in WEIGHTS.keys():
                db_key = key.replace('_', '-')
                value = record.get(db_key)
                
                try:
                    if value is not None:
                        num = float(value)
                        scores[key] = num
                    else:
                        scores[key] = None
                except:
                    scores[key] = None
            
            # Разделяем валидные (>0) и невалидные
            valid = {}
            invalid_weight = 0.0
            
            for key, weight in WEIGHTS.items():
                score = scores.get(key)
                if score is not None and score > 0:
                    valid[key] = {'score': score, 'weight': weight}
                else:
                    invalid_weight += weight
            
            if not valid:
                return 5.0
            
            # Перераспределяем веса
            if invalid_weight > 0:
                weight_per_valid = invalid_weight / len(valid)
                for data in valid.values():
                    data['weight'] += weight_per_valid
            
            # Рассчитываем
            total_score = 0.0
            total_weight = 0.0
            
            for data in valid.values():
                total_score += data['score'] * data['weight']
                total_weight += data['weight']
            
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