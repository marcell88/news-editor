# services/mt_balancer.py
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

class MTBalancer:
    """Служба для среднесрочного (MT) балансирования данных в таблице state"""
    
    def __init__(self):
        self.mt_posts = int(os.getenv("MT_POSTS", 20))
        
    async def run_analysis(self):
        """Основной метод MT-анализа и обновления"""
        try:
            logger.info("🚀 Запуск MT-балансирования...")
            
            # 1. Подключаемся к БД
            pool = await Database.get_pool()
            
            # 2. Получаем последние MT_POSTS публикаций
            recent_posts = await self._get_recent_posts(pool)
            if not recent_posts:
                logger.warning("❌ Нет данных для MT-анализа")
                return
            
            logger.info(f"📊 Получено {len(recent_posts)} публикаций для MT-анализа")
            
            # 3. Извлекаем данные из публикаций
            topics, moods, authors = self._extract_data_from_posts(recent_posts)
            
            # 4. Анализируем данные через DeepSeek API
            topic_analysis = await self._analyze_topics(topics) if topics else None
            mood_analysis = await self._analyze_moods(moods) if moods else None
            author_analysis = await self._analyze_authors(authors) if authors else None
            
            # 5. Обновляем таблицу state
            await self._update_state_table(pool, topic_analysis, mood_analysis, author_analysis)
            
            # 6. Оцениваем записи editor с mt = false
            await self._evaluate_editor_records(pool, topic_analysis, mood_analysis, author_analysis)
            
            logger.info("✅ MT-балансирование успешно завершено")
            
        except Exception as e:
            logger.error(f"❌ Ошибка при MT-балансировании: {e}")
            raise
    
    async def _get_recent_posts(self, pool) -> List[Dict]:
        """Получает последние MT_POSTS публикаций из таблица published"""
        try:
            async with pool.acquire() as conn:
                query = """
                SELECT id, topic, mood, author 
                FROM published 
                WHERE topic IS NOT NULL 
                  AND mood IS NOT NULL 
                  AND author IS NOT NULL
                  AND topic != '' 
                  AND mood != '' 
                  AND author != ''
                ORDER BY id DESC 
                LIMIT $1
                """
                
                rows = await conn.fetch(query, self.mt_posts)
                return [dict(row) for row in rows]
                
        except Exception as e:
            logger.error(f"Ошибка при получении публикаций: {e}")
            return []
    
    def _extract_data_from_posts(self, posts: List[Dict]) -> Tuple[List[str], List[str], List[str]]:
        """Извлекает темы, настроения и авторов из публикаций"""
        all_topics = []
        all_moods = []
        all_authors = []
        
        for post in posts:
            # Извлекаем темы
            if post.get('topic'):
                topics = [t.strip() for t in post['topic'].split(',') if t.strip()]
                all_topics.extend(topics)
            
            # Извлекаем настроения
            if post.get('mood'):
                moods = [m.strip() for m in post['mood'].split(',') if m.strip()]
                all_moods.extend(moods)
            
            # Извлекаем авторов
            if post.get('author'):
                authors = [a.strip() for a in post['author'].split(',') if a.strip()]
                all_authors.extend(authors)
        
        logger.info(f"📋 Извлечено: {len(all_topics)} тем, {len(all_moods)} настроений, {len(all_authors)} авторов")
        return all_topics, all_moods, all_authors
    
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
                logger.info(f"📊 Получено {len(topic_categories)} MT-категорий тем")
                return topic_categories
            else:
                logger.error("Неверный формат ответа от DeepSeek для MT-тем")
                return None
                
        except Exception as e:
            logger.error(f"Ошибка анализа MT-тем через DeepSeek: {e}")
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
                logger.info(f"😊 Получено {len(mood_categories)} MT-категорий настроений")
                return mood_categories
            else:
                logger.error("Неверный формат ответа от DeepSeek для MT-настроений")
                return None
                
        except Exception as e:
            logger.error(f"Ошибка анализа MT-настроений через DeepSeek: {e}")
            return None
    
    async def _analyze_authors(self, authors: List[str]) -> Optional[List[Dict[str, Any]]]:
        """Анализирует авторов через DeepSeek API"""
        try:
            if not authors:
                return None
                
            authors_text = "\n".join([f"- {author}" for author in authors])
            
            result = await call_deepseek_api(
                prompt=prompts.MT_AUTHOR_ANALYSIS_PROMPT,
                text=authors_text,
                response_schema=prompts.MT_AUTHOR_SCHEMA,
                model_type="deepseek-chat",
                temperature=0.3,
                tokens=500
            )
            
            if result and "author_categories" in result:
                author_categories = result["author_categories"]
                logger.info(f"👤 Получено {len(author_categories)} MT-категорий авторов")
                return author_categories
            else:
                logger.error("Неверный формат ответа от DeepSeek для MT-авторов")
                return None
                
        except Exception as e:
            logger.error(f"Ошибка анализа MT-авторов через DeepSeek: {e}")
            return None
    
    async def _update_state_table(self, pool, topic_analysis, mood_analysis, author_analysis):
        """Обновляет MT-данные в таблице state"""
        try:
            async with pool.acquire() as conn:
                # Проверяем, существует ли запись в state
                check_query = "SELECT COUNT(*) as count FROM state"
                count_result = await conn.fetchval(check_query)
                
                update_fields = []
                params = []
                param_counter = 1
                
                # Подготавливаем данные для обновления
                if topic_analysis:
                    mt_topic_array = [json.dumps(item, ensure_ascii=False) for item in topic_analysis]
                    update_fields.append(f'"mt-topic" = ${param_counter}')
                    params.append(mt_topic_array)
                    param_counter += 1
                    logger.info("📋 Обновление mt-topic:")
                    logger.info(json.dumps(topic_analysis, ensure_ascii=False, indent=2))
                
                if mood_analysis:
                    mt_mood_array = [json.dumps(item, ensure_ascii=False) for item in mood_analysis]
                    update_fields.append(f'"mt-mood" = ${param_counter}')
                    params.append(mt_mood_array)
                    param_counter += 1
                    logger.info("😊 Обновление mt-mood:")
                    logger.info(json.dumps(mood_analysis, ensure_ascii=False, indent=2))
                
                if author_analysis:
                    mt_author_array = [json.dumps(item, ensure_ascii=False) for item in author_analysis]
                    update_fields.append(f'"mt-author" = ${param_counter}')
                    params.append(mt_author_array)
                    param_counter += 1
                    logger.info("👤 Обновление mt-author:")
                    logger.info(json.dumps(author_analysis, ensure_ascii=False, indent=2))
                
                if not update_fields:
                    logger.warning("Нет MT-данных для обновления")
                    return
                
                if count_result == 0:
                    # Создаем новую запись
                    columns = []
                    placeholders = []
                    for i, field in enumerate(update_fields, 1):
                        column_name = field.split('=')[0].strip().strip('"')
                        columns.append(f'"{column_name}"')
                        placeholders.append(f"${i}")
                    
                    insert_query = f"""
                    INSERT INTO state ({', '.join(columns)})
                    VALUES ({', '.join(placeholders)})
                    """
                    
                    await conn.execute(insert_query, *params)
                    logger.info("✅ Создана новая запись в таблице state с MT-данными")
                else:
                    # Обновляем существующую запись
                    update_query = f"""
                    UPDATE state 
                    SET {', '.join(update_fields)}
                    WHERE id = (SELECT id FROM state ORDER BY id DESC LIMIT 1)
                    """
                    
                    await conn.execute(update_query, *params)
                    logger.info("✅ Обновлена запись в таблице state с MT-данными")
                
                # Логируем итоги
                if topic_analysis:
                    logger.info(f"📊 Сохранено {len(topic_analysis)} MT-категорий тем")
                if mood_analysis:
                    logger.info(f"😊 Сохранено {len(mood_analysis)} MT-категорий настроений")
                if author_analysis:
                    logger.info(f"👤 Сохранено {len(author_analysis)} MT-категорий авторов")
                    
        except Exception as e:
            logger.error(f"❌ Ошибка обновления таблицы state: {e}")
            raise
    
    async def _evaluate_editor_records(self, pool, topic_analysis, mood_analysis, author_analysis):
        """Оценивает записи editor с mt = false"""
        try:
            async with pool.acquire() as conn:
                # Получаем ТОЛЬКО записи с mt = false
                query_records = """
                SELECT id, topic, mood, author 
                FROM editor 
                WHERE mt = false
                ORDER BY id
                """
                
                records = await conn.fetch(query_records)
                
                if not records:
                    logger.info("✅ В таблице editor нет записей с mt = false для оценки")
                    return
                
                logger.info(f"🔍 Найдено {len(records)} записей с mt = false для оценки")
                
                # Оцениваем записи
                evaluated_count = 0
                
                for record in records:
                    try:
                        await self._evaluate_single_record(conn, record, topic_analysis, mood_analysis, author_analysis)
                        evaluated_count += 1
                        
                        # Пауза между запросами к API
                        await asyncio.sleep(1.5)
                            
                    except Exception as e:
                        logger.error(f"❌ Ошибка при оценке записи ID {record['id']}: {e}")
                        # Если ошибка, оставляем mt=false
                
                logger.info(f"✅ Оценено {evaluated_count} записей editor по MT-критериям")
                
        except Exception as e:
            logger.error(f"❌ Ошибка при оценке записей editor: {e}")
    
    async def _evaluate_single_record(self, conn, record, topic_analysis, mood_analysis, author_analysis):
        """Оценивает одну запись editor"""
        record_id = record['id']
        topic_text = record['topic']
        mood_text = record['mood']
        author_text = record['author']
        
        logger.info(f"🔍 Оцениваю запись editor ID {record_id}")
        
        # Проверяем наличие автора (пустая строка или NULL)
        if not author_text or (isinstance(author_text, str) and author_text.strip() == ''):
            # Автор отсутствует - ставим -1 только для mt-author
            author_score = -1
            logger.info(f"🚫 Запись {record_id}: автор отсутствует, mt-author = -1")
        else:
            # Автор есть - оцениваем его
            if author_analysis:
                author_score = await self._evaluate_author_diversification(author_text, author_analysis)
            else:
                author_score = 5  # Без данных для анализа - средняя оценка
        
        # Оценка темы (если есть данные для анализа)
        if topic_analysis:
            topic_score = await self._evaluate_topic_diversification(topic_text, topic_analysis) if topic_text else 5
        else:
            topic_score = 5  # Без данных для анализа - средняя оценка
        
        # Оценка настроения (если есть данные для анализа)
        if mood_analysis:
            mood_score = await self._evaluate_mood_diversification(mood_text, mood_analysis) if mood_text else 5
        else:
            mood_score = 5  # Без данных для анализа - средняя оценка
        
        # Обновляем запись в БД
        await self._update_editor_record_mt(conn, record_id, topic_score, mood_score, author_score)
        
        logger.info(f"✅ Запись {record_id}: mt-topic={topic_score}, mt-mood={mood_score}, mt-author={author_score}")
    
    async def _evaluate_topic_diversification(self, topic_text: str, topic_analysis: List[Dict]) -> int:
        """Оценивает диверсификацию темы от 1 до 10"""
        try:
            if not topic_analysis or not topic_text:
                return 5
                
            mt_topics_str = "\n".join([f"- {item['topic']} (вес: {item['weight']:.2f})" 
                                      for item in topic_analysis])
            
            result = await call_deepseek_api(
                prompt=prompts.LT_TOPIC_DIVERSIFICATION_PROMPT,
                text=f"Текущие MT-темы:\n{mt_topics_str}\n\nНовая тема: {topic_text}",
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
                logger.warning(f"Неверный ответ для оценки MT-темы, устанавливаем score=5")
                return 5
                
        except Exception as e:
            logger.error(f"Ошибка при оценке диверсификации MT-темы: {e}")
            return 5
    
    async def _evaluate_mood_diversification(self, mood_text: str, mood_analysis: List[Dict]) -> int:
        """Оценивает диверсификацию настроения от 1 до 10"""
        try:
            if not mood_analysis or not mood_text:
                return 5
                
            mt_moods_str = "\n".join([f"- {item['mood']} (вес: {item['weight']:.2f})" 
                                     for item in mood_analysis])
            
            result = await call_deepseek_api(
                prompt=prompts.LT_MOOD_DIVERSIFICATION_PROMPT,
                text=f"Текущие MT-настроения:\n{mt_moods_str}\n\nНовое настроение: {mood_text}",
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
                logger.warning(f"Неверный ответ для оценки MT-настроения, устанавливаем score=5")
                return 5
                
        except Exception as e:
            logger.error(f"Ошибка при оценке диверсификации MT-настроения: {e}")
            return 5
    
    async def _evaluate_author_diversification(self, author_text: str, author_analysis: List[Dict]) -> int:
        """Оценивает диверсификацию автора от 1 до 10"""
        try:
            if not author_analysis or not author_text:
                return 5
                
            mt_authors_str = "\n".join([f"- {item['author']} (вес: {item['weight']:.2f})" 
                                       for item in author_analysis])
            
            result = await call_deepseek_api(
                prompt=prompts.MT_AUTHOR_DIVERSIFICATION_PROMPT,
                text=f"Текущие MT-авторы:\n{mt_authors_str}\n\nНовый автор: {author_text}",
                response_schema=prompts.MT_DIVERSIFICATION_SCHEMA,
                model_type="deepseek-chat",
                temperature=0.3,
                tokens=300
            )
            
            if result and "diversification_score" in result:
                score = result["diversification_score"]
                score = max(1, min(10, int(score)))
                return score
            else:
                logger.warning(f"Неверный ответ для оценки MT-автора, устанавливаем score=5")
                return 5
                
        except Exception as e:
            logger.error(f"Ошибка при оценке диверсификации MT-автора: {e}")
            return 5
    
    async def _update_editor_record_mt(self, conn, record_id: int, topic_score: int, mood_score: int, author_score: int):
        """Обновляет запись в таблице editor с MT-оценками"""
        try:
            update_query = """
            UPDATE editor 
            SET "mt-topic" = $1, 
                "mt-mood" = $2, 
                "mt-author" = $3,
                mt = true
            WHERE id = $4
            """
            
            result = await conn.execute(update_query, topic_score, mood_score, author_score, record_id)
            logger.debug(f"✅ Запись {record_id} обновлена с оценками: topic={topic_score}, mood={mood_score}, author={author_score}")
            
        except Exception as e:
            logger.error(f"Ошибка при обновлении записи editor ID {record_id}: {e}")
            raise

async def main():
    """Основная функция службы"""
    balancer = MTBalancer()
    await balancer.run_analysis()

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    asyncio.run(main())