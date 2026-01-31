# services/preparator.py
import asyncio
import logging
import re
from typing import List, Dict, Any, Tuple, Optional
from dotenv import load_dotenv

from database.database import Database

logger = logging.getLogger(__name__)
load_dotenv()

class PreparatorService:
    """
    Служба для подготовки текстов из таблицы to_publish.
    Разделяет текст на компоненты, экранирует Markdown V2 и формирует финальный текст.
    """
    
    def __init__(self):
        self.check_interval = 10  # секунд между проверками
        
        # Специальные символы для экранирования в Markdown V2
        self.special_chars = [
            '\\', '_', '*', '[', ']', '(', ')', '~', '`',
            '>', '<', '&', '#', '+', '-', '=', '|', '{',
            '}', '.', '!'
        ]
        
    async def run_monitoring(self):
        """Основной цикл мониторинга."""
        try:
            logger.info("🛠️ Preparator Service запущен")
            
            while True:
                await self._check_and_process()
                await asyncio.sleep(self.check_interval)
                
        except asyncio.CancelledError:
            logger.info("Preparator Service остановлен")
        except Exception as e:
            logger.error(f"Ошибка в основном цикле Preparator: {e}")
            # Пауза перед перезапуском при критической ошибке
            await asyncio.sleep(30)
    
    async def _check_and_process(self):
        """Проверяет и обрабатывает записи, требующие подготовки."""
        try:
            pool = await Database.get_pool()
            
            # Получаем записи, где prepare = false
            records = await self._get_records_to_process(pool)
            if records:
                logger.info(f"Найдено {len(records)} записей для подготовки")
                await self._process_records(pool, records)
                
        except Exception as e:
            logger.error(f"Ошибка в _check_and_process: {e}")
    
    async def _get_records_to_process(self, pool) -> List[Dict]:
        """Получает записи, требующие подготовки."""
        try:
            async with pool.acquire() as conn:
                query = """
                SELECT id, text 
                FROM to_publish 
                WHERE prepare = false 
                ORDER BY id ASC
                LIMIT 10
                """
                rows = await conn.fetch(query)
                return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"Ошибка получения записей из to_publish: {e}")
            return []
    
    async def _process_records(self, pool, records: List[Dict]):
        """Обрабатывает записи: подготавливает текст и сохраняет в БД."""
        processed = 0
        failed = 0
        
        for record in records:
            try:
                record_id = record['id']
                raw_text = record['text']
                
                if not raw_text or not raw_text.strip():
                    logger.warning(f"ID {record_id}: пустой текст, пропускаем")
                    # Помечаем как обработанное
                    await self._mark_as_processed(pool, record_id, None)
                    continue
                
                logger.info(f"ID {record_id}: подготовка текста...")
                
                # Разбираем текст
                text_type, components = self._parse_text(raw_text)
                
                if not components:
                    logger.warning(f"ID {record_id}: не удалось разобрать текст")
                    await self._mark_as_processed(pool, record_id, None)
                    failed += 1
                    continue
                
                # Обрабатываем компоненты
                prepared_text = self._prepare_components(text_type, components)
                
                if prepared_text:
                    # Сохраняем в БД
                    await self._update_record(pool, record_id, prepared_text)
                    processed += 1
                    logger.info(f"✅ ID {record_id}: текст подготовлен ({text_type})")
                else:
                    await self._mark_as_processed(pool, record_id, None)
                    failed += 1
                    logger.error(f"❌ ID {record_id}: не удалось подготовить текст")
                    
            except Exception as e:
                failed += 1
                logger.error(f"❌ ID {record['id']}: ошибка обработки: {e}")
        
        if processed or failed:
            logger.info(f"🛠️ Обработано: {processed} успешно, {failed} с ошибками")
    
    def _parse_text(self, raw_text: str) -> Tuple[str, Dict[str, str]]:
        """Разбирает текст на компоненты по разделителю '1111'."""
        # Разделяем текст по разделителю (с учетом разных вариантов написания)
        parts = [part.strip() for part in re.split(r'1111\s*', raw_text.strip())]
        
        components = {}
        
        if len(parts) == 2:
            # Короткий формат: original и link
            text_type = "short"
            components["original"] = parts[0]
            components["link"] = parts[1]
            
        elif len(parts) == 4:
            # Длинный формат: original, link, title, output
            text_type = "long"
            components["original"] = parts[0]
            components["link"] = parts[1]
            components["title"] = parts[2]
            components["output"] = parts[3]
            
        else:
            # Неизвестный формат
            text_type = "unknown"
            logger.warning(f"Неизвестный формат текста: {len(parts)} частей")
            return text_type, {}
        
        # Чистим компоненты - убираем пробелы только в начале и конце строк
        for key, value in components.items():
            # Разделяем на строки
            lines = value.split('\n')
            # Убираем пробелы только в начале и конце каждой строки
            cleaned_lines = [line.strip() for line in lines]
            # Объединяем обратно
            components[key] = '\n'.join(cleaned_lines)
        
        return text_type, components
    
    def _escape_markdown(self, text: str) -> str:
        """Экранирует специальные символы для Markdown V2."""
        if not text:
            return ""
        
        for char in self.special_chars:
            text = text.replace(char, f'\\{char}')
        return text
    
    def _paragraph_quote(self, text: str) -> str:
        """Форматирует текст как цитату (каждая параграф с >)."""
        if not text:
            return ""
        
        # Разделяем на параграфы (пустые строки как разделители)
        paragraphs = [para.strip() for para in text.split('\n\n') if para.strip()]
        # Каждый параграф начинается с >
        quoted_paragraphs = []
        for para in paragraphs:
            # Если параграф состоит из нескольких строк, обрабатываем каждую строку
            lines = [line.strip() for line in para.split('\n') if line.strip()]
            quoted_lines = []
            for line in lines:
                quoted_lines.append(f'>{line}')
            
            if len(quoted_lines) > 1:
                # Несколько строк в параграфе
                quoted_paragraphs.append('\n>'.join(quoted_lines))
            else:
                # Одна строка в параграфе
                quoted_paragraphs.append(quoted_lines[0])
        
        return '\n>\n'.join(quoted_paragraphs)
    
    def _prepare_components(self, text_type: str, components: Dict[str, str]) -> Optional[str]:
        """Подготавливает финальный текст для публикации."""
        try:
            # Экранируем оригинальный текст
            original_escaped = self._escape_markdown(components["original"])
            link = components["link"]
            
            if text_type == "short":
                # Короткий формат: только original + link
                output_part = ""
                
            elif text_type == "long":
                # Длинный формат: добавляем title + output как цитату
                title = components.get("title", "")
                output = components.get("output", "")
                
                # Объединяем title и output
                combined = f"{title}\n\n{output}" if title else output
                # Цитируем результат
                output_part = self._paragraph_quote(self._escape_markdown(combined))
                
            else:
                logger.error(f"Неизвестный тип текста: {text_type}")
                return None
            
            # Формируем финальный текст
            text_parts = []
            
            # Добавляем original
            text_parts.append(original_escaped)
            
            # Добавляем output (если есть)
            if output_part:
                text_parts.append("")
                text_parts.append(output_part)
            
            # Добавляем разделитель
            text_parts.append("")
            text_parts.append("")
            
            # Добавляем ссылки
            text_parts.append(f"[Оригинал]({link})")
            text_parts.append("[Подписаться](https://t.me/news_anthology)")
            
            return '\n'.join(text_parts)
            
        except Exception as e:
            logger.error(f"Ошибка подготовки компонентов: {e}")
            return None
    
    async def _update_record(self, pool, record_id: int, prepared_text: str):
        """Обновляет запись в БД с подготовленным текстом."""
        try:
            async with pool.acquire() as conn:
                query = """
                UPDATE to_publish 
                SET 
                    text_prepared = $1,
                    prepare = true
                WHERE id = $2
                """
                await conn.execute(query, prepared_text, record_id)
        except Exception as e:
            logger.error(f"Ошибка обновления записи ID {record_id}: {e}")
            raise
    
    async def _mark_as_processed(self, pool, record_id: int, prepared_text: str = None):
        """Помечает запись как обработанную (даже если текст не подготовлен)."""
        try:
            async with pool.acquire() as conn:
                if prepared_text:
                    query = """
                    UPDATE to_publish 
                    SET 
                        text_prepared = $1,
                        prepare = true
                    WHERE id = $2
                    """
                    await conn.execute(query, prepared_text, record_id)
                else:
                    query = """
                    UPDATE to_publish 
                    SET 
                        prepare = true
                    WHERE id = $1
                    """
                    await conn.execute(query, record_id)
        except Exception as e:
            logger.error(f"Ошибка пометки записи ID {record_id} как обработанной: {e}")


async def main():
    """Тестовый запуск службы."""
    preparator = PreparatorService()
    await preparator.run_monitoring()

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    asyncio.run(main())