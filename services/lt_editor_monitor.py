# services/lt_editor_monitor.py
import asyncio
import logging
import os
from typing import List, Optional, Dict, Any, Tuple
import json
from dotenv import load_dotenv

from database.database import Database
from utils.deepseek_service import call_deepseek_api
import prompts

logger = logging.getLogger(__name__)

# Загрузка переменных окружения
load_dotenv()

class LTEditorMonitor:
    """Служба для постоянного мониторинга и оценки диверсификации editor"""
    
    def __init__(self):
        self.check_interval = 60  # Проверка каждые 60 секунд
        self.batch_size = 5  # Обрабатывать по 5 записей за раз
        
    async def run_monitoring(self):
        """Основной метод мониторинга"""
        try:
            logger.info("👁️ Запуск мониторинга редакторских постов...")
            
            while True:
                await self._check_and_process_editor_records()
                await asyncio.sleep(self.check_interval)
                
        except Exception as e:
            logger.error(f"❌ Критическая ошибка в мониторинге: {e}")
    
    async def _check_and_process_editor_records(self):
        """Проверяет и обрабатывает неоцененные записи editor"""
        try:
            pool = await Database.get_pool()
            async with pool.acquire() as conn:
                # 1. Получаем текущие LT-данные
                lt_data = await self._get_current_lt_data(conn)
                if not lt_data:
                    logger.debug("⏳ Нет LT-данных для оценки, ждем...")
                    return
                
                lt_topics, lt_moods = lt_data
                
                # 2. Получаем порцию необработанных записей
                editor_records = await self._get_unprocessed_editor_records(conn)
                if not editor_records:
                    logger.debug("✅ Все записи editor уже обработаны")
                    return
                
                logger.info(f"📝 Найдено {len(editor_records)} записей для оценки")
                
                # 3. Оцениваем записи партиями
                processed_count = 0
                for record in editor_records[:self.batch_size]:
                    try:
                        await self._evaluate_single_record(conn, record, lt_topics, lt_moods)
                        processed_count += 1
                        
                        # Пауза между запросами к API
                        await asyncio.sleep(1)
                            
                    except Exception as e:
                        logger.error(f"❌ Ошибка при оценке записи ID {record['id']}: {e}")
                
                if processed_count > 0:
                    logger.info(f"✅ Оценено {processed_count} записей editor")
                
        except Exception as e:
            logger.error(f"❌ Ошибка при проверке записей editor: {e}")
    
    async def _get_current_lt_data(self, conn) -> Optional[Tuple[List[Dict], List[Dict]]]:
        """Получает текущие LT-данные из таблицы state"""
        try:
            query = """
            SELECT "lt-topic", "lt-mood" 
            FROM state 
            ORDER BY id DESC 
            LIMIT 1
            """
            
            row = await conn.fetchrow(query)
            if not row:
                return None
            
            # Парсим JSON данные
            lt_topics = []
            lt_moods = []
            
            if row['lt-topic']:
                for item in row['lt-topic']:
                    try:
                        lt_topics.append(json.loads(item))
                    except:
                        continue
            
            if row['lt-mood']:
                for item in row['lt-mood']:
                    try:
                        lt_moods.append(json.loads(item))
                    except:
                        continue
            
            if lt_topics or lt_moods:
                logger.debug(f"📊 Загружены LT-данные: {len(lt_topics)} тем, {len(lt_moods)} настроений")
            
            return lt_topics, lt_moods
            
        except Exception as e:
            logger.error(f"Ошибка при получении LT-данных: {e}")
            return None
    
    async def _get_unprocessed_editor_records(self, conn):
        """Получает необработанные записи из таблицы editor"""
        try:
            query = """
            SELECT id, topic, mood 
            FROM editor 
            WHERE lt = false 
            AND topic IS NOT NULL 
            AND mood IS NOT NULL
            ORDER BY id
            LIMIT $1
            """
            
            return await conn.fetch(query, self.batch_size)
            
        except Exception as e:
            logger.error(f"Ошибка при получении записей editor: {e}")
            return []
    
    async def _evaluate_single_record(self, conn, record, lt_topics: List[Dict], lt_moods: List[Dict]):
        """Оценивает диверсификацию для одной записи editor"""
        record_id = record['id']
        topic_text = record['topic']
        mood_text = record['mood']
        
        logger.info(f"🔍 Оцениваю запись editor ID {record_id}")
        
        # 1. Оцениваем диверсификацию темы
        topic_score = await self._evaluate_topic_diversification(topic_text, lt_topics)
        
        # 2. Оцениваем диверсификацию настроения
        mood_score = await self._evaluate_mood_diversification(mood_text, lt_moods)
        
        # 3. Обновляем запись в БД
        await self._update_editor_record(conn, record_id, topic_score, mood_score)
        
        logger.info(f"✅ Запись {record_id}: topic_score={topic_score}, mood_score={mood_score}")
    
    async def _evaluate_topic_diversification(self, topic_text: str, lt_topics: List[Dict]) -> int:
        """Оценивает диверсификацию темы от 1 до 10"""
        try:
            if not lt_topics:
                return 5
                
            lt_topics_str = "\n".join([f"- {item['topic']} (вес: {item['weight']:.2f})" 
                                      for item in lt_topics])
            
            result = await call_deepseek_api(
                prompt=prompts.LT_TOPIC_DIVERSIFICATION_PROMPT,
                text=f"Текущие LT-темы:\n{lt_topics_str}\n\nНовая тема: {topic_text}",
                response_schema=prompts.LT_DIVERSIFICATION_SCHEMA,
                model_type="deepseek-chat",
                temperature=0.3,
                tokens=300
            )
            
            if result and "diversification_score" in result:
                score = result["diversification_score"]
                score = max(1, min(10, int(score)))
                return score
            else:
                logger.warning(f"Неверный ответ для оценки темы, устанавливаем score=5")
                return 5
                
        except Exception as e:
            logger.error(f"Ошибка при оценке диверсификации темы: {e}")
            return 5
    
    async def _evaluate_mood_diversification(self, mood_text: str, lt_moods: List[Dict]) -> int:
        """Оценивает диверсификацию настроения от 1 до 10"""
        try:
            if not lt_moods:
                return 5
                
            lt_moods_str = "\n".join([f"- {item['mood']} (вес: {item['weight']:.2f})" 
                                     for item in lt_moods])
            
            result = await call_deepseek_api(
                prompt=prompts.LT_MOOD_DIVERSIFICATION_PROMPT,
                text=f"Текущие LT-настроения:\n{lt_moods_str}\n\nНовое настроение: {mood_text}",
                response_schema=prompts.LT_DIVERSIFICATION_SCHEMA,
                model_type="deepseek-chat",
                temperature=0.3,
                tokens=300
            )
            
            if result and "diversification_score" in result:
                score = result["diversification_score"]
                score = max(1, min(10, int(score)))
                return score
            else:
                logger.warning(f"Неверный ответ для оценки настроения, устанавливаем score=5")
                return 5
                
        except Exception as e:
            logger.error(f"Ошибка при оценке диверсификации настроения: {e}")
            return 5
    
    async def _update_editor_record(self, conn, record_id: int, topic_score: int, mood_score: int):
        """Обновляет запись в таблице editor с оценками диверсификации"""
        try:
            update_query = """
            UPDATE editor 
            SET "lt-topic" = $1, 
                "lt-mood" = $2, 
                lt = true
            WHERE id = $3
            """
            
            await conn.execute(update_query, topic_score, mood_score, record_id)
            
        except Exception as e:
            logger.error(f"Ошибка при обновлении записи editor ID {record_id}: {e}")
            raise

async def main():
    """Основная функция службы"""
    monitor = LTEditorMonitor()
    await monitor.run_monitoring()

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    asyncio.run(main())