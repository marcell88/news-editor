# services/st_catcher.py
import asyncio
import logging
from typing import List, Dict

from database.database import Database
from utils.deepseek_service import call_deepseek_api
import prompts

logger = logging.getLogger(__name__)

class STCatcher:
    """Служба для краткосрочного (ST) анализа диверсификации контента"""
    
    def __init__(self):
        self.delay_seconds = 60  # Задержка между циклами проверки
        
    async def run_monitoring(self):
        """Основной цикл мониторинга (запускается в фоне)"""
        logger.info("🚀 Запуск ST-Catcher службы мониторинга")
        
        while True:
            try:
                await self.run_analysis()
                
                # Пауза перед следующим циклом
                logger.debug(f"⏳ ST-Catcher ожидает {self.delay_seconds} сек до следующей проверки")
                await asyncio.sleep(self.delay_seconds)
                
            except asyncio.CancelledError:
                logger.info("🛑 ST-Catcher служба остановлена")
                break
            except Exception as e:
                logger.error(f"❌ Ошибка в цикле ST-Catcher: {e}")
                await asyncio.sleep(30)  # Пауза при ошибке
    
    async def run_analysis(self):
        """Основной метод ST-анализа потенциальных постов"""
        try:
            logger.info("🔍 Запуск ST-анализа...")
            
            # 1. Подключаемся к БД
            pool = await Database.get_pool()
            
            # 2. Получаем последние 2 опубликованных поста
            last_posts = await self._get_last_published_posts(pool)
            if len(last_posts) < 2:
                logger.warning("⚠️ Недостаточно опубликованных постов для анализа (нужно минимум 2)")
                return
            
            logger.info(f"📊 Получено {len(last_posts)} опубликованных постов для анализа")
            
            # 3. Получаем все записи из editor с st = false
            editor_records = await self._get_editor_records(pool)
            if not editor_records:
                logger.info("✅ Нет записей в editor с st = false для анализа")
                return
            
            logger.info(f"📝 Найдено {len(editor_records)} записей в editor для оценки")
            
            # 4. Оцениваем каждую запись
            evaluated_count = 0
            for record in editor_records:
                try:
                    await self._evaluate_single_record(pool, record, last_posts)
                    evaluated_count += 1
                    
                    # Пауза между запросами к API
                    await asyncio.sleep(1.5)
                    
                except Exception as e:
                    logger.error(f"❌ Ошибка при оценке записи ID {record['id']}: {e}")
            
            logger.info(f"✅ ST-анализ завершен. Оценено записей: {evaluated_count}")
            
        except Exception as e:
            logger.error(f"❌ Ошибка при ST-анализе: {e}")
    
    async def _get_last_published_posts(self, pool) -> List[Dict]:
        """Получает последние 2 опубликованных поста из таблицы published"""
        try:
            async with pool.acquire() as conn:
                query = """
                SELECT id, text, topic 
                FROM published 
                WHERE text IS NOT NULL AND topic IS NOT NULL
                ORDER BY id DESC 
                LIMIT 2
                """
                
                rows = await conn.fetch(query)
                result = []
                
                for row in rows:
                    post_data = dict(row)
                    
                    # Обрабатываем текст: берем часть до разделителя "1111"
                    full_text = post_data.get('text', '')
                    if '1111' in full_text:
                        post_data['text_for_analysis'] = full_text.split('1111')[0].strip()
                    else:
                        post_data['text_for_analysis'] = full_text.strip()
                    
                    # Для анализа объединяем text и topic
                    topic_text = post_data.get('topic', '')
                    post_data['combined_text'] = f"{post_data['text_for_analysis']}\n\nТема: {topic_text}"
                    
                    result.append(post_data)
                
                return result
                
        except Exception as e:
            logger.error(f"Ошибка при получении опубликованных постов: {e}")
            return []
    
    async def _get_editor_records(self, pool) -> List[Dict]:
        """Получает все записи из editor с st = false"""
        try:
            async with pool.acquire() as conn:
                query = """
                SELECT id, text, topic 
                FROM editor 
                WHERE st = false
                ORDER BY id
                """
                
                rows = await conn.fetch(query)
                result = []
                
                for row in rows:
                    record_data = dict(row)
                    
                    # Обрабатываем текст: берем часть до разделителя "1111"
                    full_text = record_data.get('text', '')
                    if '1111' in full_text:
                        record_data['text_for_analysis'] = full_text.split('1111')[0].strip()
                    else:
                        record_data['text_for_analysis'] = full_text.strip()
                    
                    # Для анализа объединяем text и topic
                    topic_text = record_data.get('topic', '')
                    record_data['combined_text'] = f"{record_data['text_for_analysis']}\n\nТема: {topic_text}"
                    
                    result.append(record_data)
                
                return result
                
        except Exception as e:
            logger.error(f"Ошибка при получении записей из editor: {e}")
            return []
    
    async def _evaluate_single_record(self, pool, record: Dict, last_posts: List[Dict]):
        """Оценивает одну запись из editor"""
        record_id = record['id']
        logger.info(f"🔍 Оцениваю запись editor ID {record_id}")
        
        # Подготавливаем тексты для анализа
        last_post = last_posts[0]  # Самый последний
        second_last_post = last_posts[1]  # Предпоследний
        
        # Формируем текст для отправки в DeepSeek
        analysis_text = f"""Последний опубликованный пост:
{last_post['combined_text']}

Предпоследний опубликованный пост:
{second_last_post['combined_text']}

Потенциальный новый пост:
{record['combined_text']}"""
        
        # Вызываем DeepSeek API с промптом из prompts.py
        result = await call_deepseek_api(
            prompt=prompts.ST_PROMPT,
            text=analysis_text,
            response_schema=prompts.ST_PROMPT_SCHEMA,
            model_type="deepseek-chat",
            temperature=0.3,
            tokens=500
        )
        
        if result and "diversification_score" in result:
            score = result["diversification_score"]
            reasoning = result.get("reasoning", "")
            
            # Убеждаемся, что score - целое число от 0 до 10
            try:
                score = int(score)
                score = max(0, min(10, score))
            except (ValueError, TypeError):
                logger.warning(f"Некорректный score от API: {score}, устанавливаю 0")
                score = 0
            
            logger.info(f"✅ Запись {record_id}: оценка = {score}, причина: {reasoning}")
            
            # Обновляем запись в БД
            await self._update_editor_record(pool, record_id, score)
        else:
            logger.error(f"❌ Неверный ответ от DeepSeek для записи {record_id}")
            # В случае ошибки ставим score = 0 и помечаем как обработанное
            await self._update_editor_record(pool, record_id, 0)
    
    async def _update_editor_record(self, pool, record_id: int, score: int):
        """Обновляет запись в таблице editor с ST-оценкой"""
        try:
            async with pool.acquire() as conn:
                update_query = """
                UPDATE editor 
                SET "st-score" = $1, 
                    st = true
                WHERE id = $2
                """
                
                await conn.execute(update_query, score, record_id)
                logger.debug(f"✅ Запись {record_id} обновлена: st-score={score}, st=true")
                
        except Exception as e:
            logger.error(f"Ошибка при обновлении записи editor ID {record_id}: {e}")
            raise