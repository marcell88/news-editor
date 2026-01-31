# services/lt_state_updater.py
import asyncio
import logging
import os
from typing import List, Optional, Dict, Any
import json
import math
from dotenv import load_dotenv

from database.database import Database
from utils.deepseek_service import call_deepseek_api
import prompts

logger = logging.getLogger(__name__)

# Загрузка переменных окружения
load_dotenv()

class LTStateUpdater:
    """Служба для обновления LT-данных в таблице state (раз в сутки)"""

    def __init__(self):
        self.lt_posts = int(os.getenv("LT_POSTS", 50))
        
        # Читаем дополнительные параметры из .env
        self.per_hour = int(os.getenv("PER_HOUR", 300))  # постов в час
        self.min_hour = int(os.getenv("MIN", 9))         # минимальный час работы
        self.max_hour = int(os.getenv("MAX", 21))        # максимальный час работы
        
        # Рассчитываем temp по формуле: PER_HOUR * (MAX - MIN) / 700
        hours_range = self.max_hour - self.min_hour
        temp = self.per_hour * hours_range / 700
        
        # Рассчитываем периодичность: LT_POSTS / temp * 24
        update_interval_hours_raw = (self.lt_posts / temp) * 24
        
        # Округляем до целого
        self.update_interval_hours = round(update_interval_hours_raw)
        self.update_interval_seconds = self.update_interval_hours * 3600
        
        logger.info(f"⚙️ Настройки обновления:")
        logger.info(f"   LT_POSTS: {self.lt_posts}")
        logger.info(f"   PER_HOUR: {self.per_hour}")
        logger.info(f"   MIN: {self.min_hour}, MAX: {self.max_hour}")
        logger.info(f"   Часовой диапазон: {hours_range} часов")
        logger.info(f"   Temp = {self.per_hour} * {hours_range} / 700 = {temp:.2f}")
        logger.info(f"   Периодичность = {self.lt_posts} / {temp:.2f} * 24 = {update_interval_hours_raw:.2f} часов")
        logger.info(f"   Итоговая периодичность: {self.update_interval_hours} часов ({self.update_interval_seconds} секунд)")

    async def run_analysis(self):
        """Основной метод анализа тем и настроений"""
        try:
            logger.info("🚀 Запуск обновления LT-данных в state...")
            
            # 1. Подключаемся к БД
            pool = await Database.get_pool()
            
            # 2. Получаем темы из последних публикаций
            topics = await self._get_recent_topics(pool)
            moods = await self._get_recent_moods(pool)
            
            # 3. Анализируем темы через DeepSeek API
            topic_analysis = await self._analyze_topics(topics) if topics else None
            
            # 4. Анализируем настроения через DeepSeek API
            mood_analysis = await self._analyze_moods(moods) if moods else None
            
            # 5. Сохраняем в таблицу state
            await self._save_analysis_to_db(pool, topic_analysis, mood_analysis)
            
            # 6. Сбрасываем флаг lt в таблице editor для повторной оценки
            await self._reset_editor_lt_flag(pool)
            
            logger.info("✅ Обновление LT-данных успешно завершено")
            
        except Exception as e:
            logger.error(f"❌ Ошибка при обновлении LT-данных: {e}")
    
    async def _get_recent_topics(self, pool) -> List[str]:
        """Получает темы из последних LT_POSTS публикаций"""
        try:
            async with pool.acquire() as conn:
                query = """
                SELECT topic 
                FROM published 
                WHERE topic IS NOT NULL AND topic != ''
                ORDER BY id DESC 
                LIMIT $1
                """
                
                rows = await conn.fetch(query, self.lt_posts)
                all_topics = []
                
                for row in rows:
                    topic_string = row['topic']
                    if topic_string:
                        topics_in_row = [t.strip() for t in topic_string.split(',') if t.strip()]
                        all_topics.extend(topics_in_row)
                
                unique_topics = list(set(all_topics))
                
                if unique_topics:
                    logger.info(f"📊 Получено {len(unique_topics)} уникальных тем для анализа")
                
                return unique_topics
                
        except Exception as e:
            logger.error(f"Ошибка при получении тем: {e}")
            return []
    
    async def _get_recent_moods(self, pool) -> List[str]:
        """Получает настроения из последних LT_POSTS публикаций"""
        try:
            async with pool.acquire() as conn:
                query = """
                SELECT mood 
                FROM published 
                WHERE mood IS NOT NULL AND mood != ''
                ORDER BY id DESC 
                LIMIT $1
                """
                
                rows = await conn.fetch(query, self.lt_posts)
                all_moods = []
                
                for row in rows:
                    mood_string = row['mood']
                    if mood_string:
                        moods_in_row = [m.strip() for m in mood_string.split(',') if m.strip()]
                        all_moods.extend(moods_in_row)
                
                unique_moods = list(set(all_moods))
                
                if unique_moods:
                    logger.info(f"😊 Получено {len(unique_moods)} уникальных настроений для анализа")
                
                return unique_moods
                
        except Exception as e:
            logger.error(f"Ошибка при получении настроений: {e}")
            return []
    
    async def _analyze_topics(self, topics: List[str]) -> Optional[List[Dict[str, Any]]]:
        """Анализирует темы через DeepSeek API"""
        try:
            if not topics:
                return None
                
            topics_text = "\n".join([f"- {topic}" for topic in topics])
            
            result = await call_deepseek_api(
                prompt=prompts.LT_TOPIC_ANALYSIS_PROMPT,
                text=topics_text,
                response_schema=prompts.LT_TOPIC_SCHEMA,
                model_type="deepseek-chat",
                temperature=0.3,
                tokens=500
            )
            
            if result and "topic_categories" in result:
                topic_categories = result["topic_categories"]
                logger.info(f"📊 Получено {len(topic_categories)} категорий тем")
                return topic_categories
            else:
                logger.error("Неверный формат ответа от DeepSeek для тем")
                return None
                
        except Exception as e:
            logger.error(f"Ошибка анализа тем через DeepSeek: {e}")
            return None
    
    async def _analyze_moods(self, moods: List[str]) -> Optional[List[Dict[str, Any]]]:
        """Анализирует настроения через DeepSeek API"""
        try:
            if not moods:
                return None
                
            moods_text = "\n".join([f"- {mood}" for mood in moods])
            
            result = await call_deepseek_api(
                prompt=prompts.LT_MOOD_ANALYSIS_PROMPT,
                text=moods_text,
                response_schema=prompts.LT_MOOD_SCHEMA,
                model_type="deepseek-chat",
                temperature=0.3,
                tokens=500
            )
            
            if result and "mood_categories" in result:
                mood_categories = result["mood_categories"]
                logger.info(f"😊 Получено {len(mood_categories)} категорий настроений")
                return mood_categories
            else:
                logger.error("Неверный формат ответа от DeepSeek для настроений")
                return None
                
        except Exception as e:
            logger.error(f"Ошибка анализа настроений через DeepSeek: {e}")
            return None
    
    async def _save_analysis_to_db(self, pool, topic_categories: Optional[List[Dict[str, Any]]], 
                                  mood_categories: Optional[List[Dict[str, Any]]]):
        """Сохраняет анализ тем и настроений в таблицу state"""
        try:
            async with pool.acquire() as conn:
                check_query = "SELECT COUNT(*) as count FROM state"
                count_result = await conn.fetchval(check_query)
                
                if count_result == 0:
                    await self._create_new_record(conn, topic_categories, mood_categories)
                else:
                    await self._update_existing_record(conn, topic_categories, mood_categories)
                    
        except Exception as e:
            logger.error(f"❌ Ошибка сохранения анализа в БД: {e}")
    
    async def _create_new_record(self, conn, topic_categories, mood_categories):
        """Создает новую запись в таблице state"""
        lt_topic_array = None
        if topic_categories:
            lt_topic_array = [json.dumps(item, ensure_ascii=False) for item in topic_categories]
            logger.info("📋 Сохранение lt-topic:")
            logger.info(json.dumps(topic_categories, ensure_ascii=False, indent=2))
        
        lt_mood_array = None
        if mood_categories:
            lt_mood_array = [json.dumps(item, ensure_ascii=False) for item in mood_categories]
            logger.info("😊 Сохранение lt-mood:")
            logger.info(json.dumps(mood_categories, ensure_ascii=False, indent=2))
        
        if lt_topic_array and lt_mood_array:
            insert_query = """
            INSERT INTO state ("lt-topic", "lt-mood")
            VALUES ($1, $2)
            """
            await conn.execute(insert_query, lt_topic_array, lt_mood_array)
        elif lt_topic_array:
            insert_query = """
            INSERT INTO state ("lt-topic")
            VALUES ($1)
            """
            await conn.execute(insert_query, lt_topic_array)
        elif lt_mood_array:
            insert_query = """
            INSERT INTO state ("lt-mood")
            VALUES ($1)
            """
            await conn.execute(insert_query, lt_mood_array)
        else:
            logger.warning("Нет данных для сохранения")
            return
            
        logger.info("✅ Создана новая запись в таблице state")
    
    async def _update_existing_record(self, conn, topic_categories, mood_categories):
        """Обновляет существующую запись в таблице state"""
        update_fields = []
        params = []
        param_counter = 1
        
        if topic_categories:
            lt_topic_array = [json.dumps(item, ensure_ascii=False) for item in topic_categories]
            update_fields.append(f'"lt-topic" = ${param_counter}')
            params.append(lt_topic_array)
            param_counter += 1
            logger.info("📋 Обновление lt-topic:")
            logger.info(json.dumps(topic_categories, ensure_ascii=False, indent=2))
        
        if mood_categories:
            lt_mood_array = [json.dumps(item, ensure_ascii=False) for item in mood_categories]
            update_fields.append(f'"lt-mood" = ${param_counter}')
            params.append(lt_mood_array)
            param_counter += 1
            logger.info("😊 Обновление lt-mood:")
            logger.info(json.dumps(mood_categories, ensure_ascii=False, indent=2))
        
        if not update_fields:
            logger.warning("Нет данных для обновления")
            return
        
        update_query = f"""
        UPDATE state 
        SET {', '.join(update_fields)}
        WHERE id = (SELECT id FROM state ORDER BY id DESC LIMIT 1)
        """
        
        await conn.execute(update_query, *params)
        logger.info("✅ Обновлена запись в таблице state")
        
        if topic_categories:
            logger.info(f"📊 Сохранено {len(topic_categories)} категорий тем")
        if mood_categories:
            logger.info(f"😊 Сохранено {len(mood_categories)} категорий настроений")
    
    async def _reset_editor_lt_flag(self, pool):
        """Сбрасывает флаг lt в таблице editor для повторной оценки"""
        try:
            async with pool.acquire() as conn:
                update_query = """
                UPDATE editor 
                SET lt = false
                WHERE lt = true
                """
                
                updated_count = await conn.execute(update_query)
                logger.info(f"🔄 Сброшен флаг lt у {updated_count.split()[1]} записей в таблице editor")
                
        except Exception as e:
            logger.error(f"❌ Ошибка при сбросе флага lt в editor: {e}")
    
    async def run_periodic(self):
        """Запускает обновление с рассчитанной периодичностью"""
        while True:
            await self.run_analysis()
            logger.info(f"⏰ Следующее обновление через {self.update_interval_hours} часов ({self.update_interval_seconds} секунд)...")
            await asyncio.sleep(self.update_interval_seconds)

async def main():
    """Основная функция службы"""
    updater = LTStateUpdater()
    await updater.run_periodic()  # вместо run_daily()

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    asyncio.run(main())