# services/calculator.py
import asyncio
import logging
import os
from typing import List, Dict, Any
from dotenv import load_dotenv

from database.database import Database

logger = logging.getLogger(__name__)

class CalculatorService:
    def __init__(self):
        self.check_interval = 5
        self.weights = {}
        self._initialize_weights()
        
    def _initialize_weights(self):
        """Инициализирует веса из переменных окружения"""
        # Сначала попробуем загрузить .env
        load_dotenv()
        
        # Логируем процесс загрузки
        env_path = '.env'
        logger.info(f"🔄 CalculatorService: Загружаем .env из {os.path.abspath(env_path)}")
        
        if os.path.exists(env_path):
            with open(env_path, 'r') as f:
                env_content = f.read()
            logger.debug(f"CalculatorService: Содержимое .env:\n{env_content}")
        
        # Пробуем получить каждую переменную
        lt_topic_val = os.getenv("LT_TOPIC_WEIGHT")
        lt_mood_val = os.getenv("LT_MOOD_WEIGHT")
        mt_topic_val = os.getenv("MT_TOPIC_WEIGHT")
        mt_mood_val = os.getenv("MT_MOOD_WEIGHT")
        mt_author_val = os.getenv("MT_AUTHOR_WEIGHT")
        time_best_val = os.getenv("TIME_BEST_WEIGHT")
        time_expire_val = os.getenv("TIME_EXPIRE_WEIGHT")
        
        logger.info("CalculatorService: Значения из переменных окружения:")
        logger.info(f"  LT_TOPIC_WEIGHT: '{lt_topic_val}'")
        logger.info(f"  LT_MOOD_WEIGHT: '{lt_mood_val}'")
        logger.info(f"  MT_TOPIC_WEIGHT: '{mt_topic_val}'")
        logger.info(f"  MT_MOOD_WEIGHT: '{mt_mood_val}'")
        logger.info(f"  MT_AUTHOR_WEIGHT: '{mt_author_val}'")
        logger.info(f"  TIME_BEST_WEIGHT: '{time_best_val}'")
        logger.info(f"  TIME_EXPIRE_WEIGHT: '{time_expire_val}'")
        
        # Выводим все переменные окружения для отладки
        all_env_vars = dict(os.environ)
        logger.debug(f"CalculatorService: Все переменные окружения: {all_env_vars}")
        
        # Устанавливаем веса с значениями по умолчанию
        self.weights = {
            "lt_topic": float(lt_topic_val) if lt_topic_val else 0.15,
            "lt_mood": float(lt_mood_val) if lt_mood_val else 0.15,
            "mt_topic": float(mt_topic_val) if mt_topic_val else 0.15,
            "mt_mood": float(mt_mood_val) if mt_mood_val else 0.15,
            "mt_author": float(mt_author_val) if mt_author_val else 0.15,
            "time_best": float(time_best_val) if time_best_val else 0.20,
            "time_expire": float(time_expire_val) if time_expire_val else 0.05,
        }
        
        # Логируем итоговые веса
        logger.info("CalculatorService: Итоговые веса:")
        for key, value in self.weights.items():
            logger.info(f"  {key}: {value}")
        
        # Проверяем сумму весов
        total_weight = sum(self.weights.values())
        logger.info(f"CalculatorService: Сумма всех весов: {total_weight:.2f}")
        if abs(total_weight - 1.0) > 0.001:
            logger.warning(f"CalculatorService: Внимание! Сумма весов ({total_weight:.2f}) не равна 1.0")
    
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
                logger.info("=" * 60)
                logger.info(f"📊 Начинаем расчет для ID {record['id']}")
                final_score = self._calculate_score(record)
                await self._update_record(pool, record['id'], final_score)
                calculated += 1
                logger.info(f"✅ Завершен расчет ID {record['id']}: итоговая оценка = {final_score:.2f}")
            except Exception as e:
                logger.error(f"❌ Ошибка расчета ID {record['id']}: {e}")
        
        if calculated:
            logger.info(f"✅ Всего рассчитано {calculated} оценок")
    
    def _calculate_score(self, record: Dict) -> float:
        """Рассчитывает итоговую оценку"""
        try:
            record_id = record.get('id', 'unknown')
            
            # Шаг 1: Выводим все исходные данные
            logger.info(f"📋 Исходные данные для ID {record_id}:")
            logger.info(f"  lt-topic: {record.get('lt-topic')}")
            logger.info(f"  lt-mood: {record.get('lt-mood')}")
            logger.info(f"  mt-topic: {record.get('mt-topic')}")
            logger.info(f"  mt-mood: {record.get('mt-mood')}")
            logger.info(f"  mt-author: {record.get('mt-author')}")
            logger.info(f"  time-best: {record.get('time-best')}")
            logger.info(f"  time-expire: {record.get('time-expire')}")
            
            # Шаг 2: Собираем оценки
            scores = {}
            logger.info("📊 Преобразование значений в числа:")
            for key in self.weights.keys():
                db_key = key.replace('_', '-')
                value = record.get(db_key)
                
                try:
                    if value is not None:
                        num = float(value)
                        scores[key] = num
                        logger.info(f"  {key} -> {db_key}: {value} -> {num}")
                    else:
                        scores[key] = None
                        logger.info(f"  {key} -> {db_key}: {value} -> None")
                except Exception as conv_e:
                    logger.warning(f"  ❌ Ошибка конвертации {key} ({value}): {conv_e}")
                    scores[key] = None
            
            # Шаг 3: Разделяем валидные (>0) и невалидные
            valid = {}
            invalid_weight = 0.0
            invalid_items = []
            
            logger.info("📈 Анализ валидности оценок:")
            for key, weight in self.weights.items():
                score = scores.get(key)
                if score is not None and score > 0:
                    valid[key] = {'score': score, 'weight': weight}
                    logger.info(f"  ✅ {key}: оценка={score:.2f}, вес={weight}")
                else:
                    invalid_weight += weight
                    invalid_items.append(key)
                    logger.info(f"  ❌ {key}: оценка={score}, вес={weight} -> добавляем к невалидным")
            
            logger.info(f"📊 Итоги анализа:")
            logger.info(f"  Валидные оценки: {len(valid)} шт")
            logger.info(f"  Невалидные оценки: {len(invalid_items)} шт: {', '.join(invalid_items)}")
            logger.info(f"  Сумма весов невалидных: {invalid_weight:.2f}")
            
            # Шаг 4: Проверяем наличие валидных оценок
            if not valid:
                logger.warning(f"⚠️ Нет валидных оценок, возвращаем значение по умолчанию 5.0")
                return 5.0
            
            # Шаг 5: Перераспределяем веса
            if invalid_weight > 0:
                weight_per_valid = invalid_weight / len(valid)
                logger.info(f"📐 Перераспределение весов:")
                logger.info(f"  Общий вес невалидных: {invalid_weight:.2f}")
                logger.info(f"  Количество валидных: {len(valid)}")
                logger.info(f"  Дополнительный вес на каждый валидный: {weight_per_valid:.3f}")
                
                for key, data in valid.items():
                    old_weight = data['weight']
                    data['weight'] += weight_per_valid
                    logger.info(f"  {key}: {old_weight:.3f} -> {data['weight']:.3f} (+{weight_per_valid:.3f})")
            
            # Шаг 6: Рассчитываем
            logger.info("🧮 Расчет итоговой оценки:")
            total_score = 0.0
            total_weight = 0.0
            
            for key, data in valid.items():
                contribution = data['score'] * data['weight']
                total_score += contribution
                total_weight += data['weight']
                logger.info(f"  {key}: {data['score']:.2f} * {data['weight']:.3f} = {contribution:.3f}")
            
            logger.info(f"📊 Суммы:")
            logger.info(f"  Сумма взвешенных оценок: {total_score:.3f}")
            logger.info(f"  Сумма весов: {total_weight:.3f}")
            
            # Шаг 7: Финальный расчет
            if total_weight > 0:
                final = total_score / total_weight
                logger.info(f"  Итоговая формула: {total_score:.3f} / {total_weight:.3f} = {final:.3f}")
            else:
                logger.warning("⚠️ Сумма весов равна 0, используем значение по умолчанию 5.0")
                final = 5.0
            
            # Шаг 8: Проверяем границы
            if final < 1.0:
                logger.info(f"⚠️ Оценка {final:.3f} ниже минимальной (1.0), устанавливаем 1.0")
                final = 1.0
            elif final > 10.0:
                logger.info(f"⚠️ Оценка {final:.3f} выше максимальной (10.0), устанавливаем 10.0")
                final = 10.0
            else:
                logger.info(f"✅ Оценка {final:.3f} в допустимых границах [1.0, 10.0]")
            
            # Шаг 9: Округляем
            final_rounded = round(final, 2)
            if final_rounded != final:
                logger.info(f"🔢 Округление: {final:.3f} -> {final_rounded:.2f}")
            
            return final_rounded
            
        except Exception as e:
            logger.error(f"❌ Критическая ошибка расчета для ID {record_id}: {e}")
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
                result = await conn.execute(query, round(final_score, 2), record_id)
                logger.info(f"💾 Сохранение в БД: ID {record_id} = {final_score:.2f}")
        except Exception as e:
            logger.error(f"❌ Ошибка обновления ID {record_id}: {e}")
            raise

async def main():
    # Настройка логирования при запуске напрямую
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    calculator = CalculatorService()
    await calculator.run_monitoring()

if __name__ == "__main__":
    asyncio.run(main())