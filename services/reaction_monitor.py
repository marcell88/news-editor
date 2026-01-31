# services/reaction_monitor.py
import asyncio
import logging
import os
import json
from datetime import datetime
import aiohttp
from dotenv import load_dotenv

logger = logging.getLogger(__name__)
load_dotenv()

class ReactionMonitor:
    """
    Упрощенный мониторинг реакций
    """
    
    def __init__(self):
        self.bot_token = os.getenv('PUBLISH_API')
        self.preview_group = os.getenv('PREVIEW_GROUP')
        
        if not self.bot_token:
            raise ValueError("Не найден PUBLISH_API в .env")
        if not self.preview_group:
            raise ValueError("Не найден PREVIEW_GROUP в .env")
        
        # Конвертируем в int для сравнения
        self.preview_group_id = int(self.preview_group)
        self.last_update_id = 0
        
        logger.info(f"Reaction Monitor для группы: {self.preview_group}")
    
    async def run_monitoring(self):
        """Основной цикл мониторинга"""
        logger.info("👁️ Reaction Monitor запущен")
        
        # Сначала отключаем webhook на всякий случай
        await self._disable_webhook()
        await asyncio.sleep(1)
        
        # Получаем текущий offset
        await self._get_current_offset()
        
        while True:
            try:
                await self._check_updates()
            except Exception as e:
                logger.error(f"Ошибка: {e}")
                await asyncio.sleep(5)
    
    async def _disable_webhook(self):
        """Отключаем webhook"""
        try:
            url = f"https://api.telegram.org/bot{self.bot_token}/deleteWebhook"
            params = {'drop_pending_updates': True}
            
            async with aiohttp.ClientSession() as session:
                async with session.get(url, params=params) as response:
                    if response.status == 200:
                        data = await response.json()
                        if data.get('ok'):
                            logger.info("✅ Webhook отключен")
                        else:
                            logger.warning(f"Webhook: {data}")
        except Exception as e:
            logger.warning(f"Webhook отключение: {e}")
    
    async def _get_current_offset(self):
        """Получаем текущий offset"""
        try:
            url = f"https://api.telegram.org/bot{self.bot_token}/getUpdates"
            params = {'offset': -1, 'limit': 1}
            
            async with aiohttp.ClientSession() as session:
                async with session.get(url, params=params) as response:
                    if response.status == 200:
                        data = await response.json()
                        if data.get('ok'):
                            updates = data.get('result', [])
                            if updates:
                                self.last_update_id = updates[-1]['update_id']
                                logger.info(f"Текущий update_id: {self.last_update_id}")
        except Exception as e:
            logger.warning(f"Offset: {e}")
    
    async def _check_updates(self):
        """Проверяем обновления"""
        url = f"https://api.telegram.org/bot{self.bot_token}/getUpdates"
        
        params = {
            'offset': self.last_update_id + 1,
            'timeout': 30,  # Long polling
            'allowed_updates': ['message_reaction', 'message_reaction_count']
        }
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, params=params, timeout=35) as response:
                    
                    if response.status == 409:  # Конфликт с webhook
                        logger.error("⚠️ Конфликт! Отключаю webhook...")
                        await self._disable_webhook()
                        await asyncio.sleep(2)
                        return
                    
                    if response.status != 200:
                        logger.error(f"HTTP ошибка: {response.status}")
                        return
                    
                    data = await response.json()
                    
                    if not data.get('ok'):
                        logger.error(f"API ошибка: {data}")
                        return
                    
                    updates = data.get('result', [])
                    
                    if updates:
                        logger.info(f"📨 Получено {len(updates)} обновлений")
                        
                        for update in updates:
                            await self._process_update(update)
                            
                            # Обновляем last_update_id
                            update_id = update.get('update_id', 0)
                            if update_id > self.last_update_id:
                                self.last_update_id = update_id
                    
        except asyncio.TimeoutError:
            # Это нормально для long polling
            pass
        except Exception as e:
            raise e
    
    async def _process_update(self, update: dict):
        """Обрабатываем обновление"""
        update_id = update.get('update_id', 0)
        
        # Отладочный вывод ВСЕХ обновлений
        logger.debug(f"Update {update_id}: {json.dumps(update, indent=2, ensure_ascii=False)[:200]}...")
        
        if 'message_reaction' in update:
            await self._handle_reaction(update['message_reaction'], update_id)
        elif 'message_reaction_count' in update:
            await self._handle_reaction_count(update['message_reaction_count'], update_id)
        else:
            # Логируем другие типы обновлений для отладки
            keys = list(update.keys())
            if 'message' in keys:
                logger.debug(f"Update {update_id}: сообщение (игнорируем)")
    
    async def _handle_reaction(self, reaction_data: dict, update_id: int):
        """Обрабатываем реакцию"""
        try:
            chat_id = reaction_data.get('chat', {}).get('id')
            
            # Проверяем нашу группу
            if chat_id != self.preview_group_id:
                logger.debug(f"Реакция не из нашей группы: {chat_id}")
                return
            
            message_id = reaction_data.get('message_id')
            user_id = reaction_data.get('user', {}).get('id')
            
            # Получаем реакции
            new_reactions = reaction_data.get('new_reaction', [])
            old_reactions = reaction_data.get('old_reaction', [])
            
            logger.info(f"🔥 Реакция! User {user_id} на сообщение {message_id}")
            
            # Анализируем 👍 и 👎
            await self._analyze_reactions(new_reactions, old_reactions, user_id, message_id)
            
        except Exception as e:
            logger.error(f"Ошибка обработки реакции: {e}")
    
    async def _handle_reaction_count(self, count_data: dict, update_id: int):
        """Обрабатываем счетчик реакций"""
        try:
            chat_id = count_data.get('chat', {}).get('id')
            
            if chat_id != self.preview_group_id:
                return
            
            message_id = count_data.get('message_id')
            reactions = count_data.get('reactions', [])
            
            # Логируем счетчики
            thumbs_up = 0
            thumbs_down = 0
            
            for reaction in reactions:
                emoji = self._get_emoji(reaction)
                count = reaction.get('count', 1)
                
                if emoji == '👍':
                    thumbs_up += count
                elif emoji == '👎':
                    thumbs_down += count
            
            if thumbs_up > 0 or thumbs_down > 0:
                logger.info(f"📊 Счетчики для сообщения {message_id}: 👍 {thumbs_up} 👎 {thumbs_down}")
                
        except Exception as e:
            logger.error(f"Ошибка счетчика: {e}")
    
    async def _analyze_reactions(self, new_reactions: list, old_reactions: list, user_id: int, message_id: int):
        """Анализируем реакции"""
        # Собираем эмодзи
        new_emojis = [self._get_emoji(r) for r in new_reactions]
        old_emojis = [self._get_emoji(r) for r in old_reactions]
        
        # Проверяем изменения 👍
        had_like = '👍' in old_emojis
        has_like = '👍' in new_emojis
        
        if had_like != has_like:
            if has_like:
                self._log_action(user_id, message_id, "поставил лайк", "👍")
            else:
                self._log_action(user_id, message_id, "убрал лайк", "👍")
        
        # Проверяем изменения 👎
        had_dislike = '👎' in old_emojis
        has_dislike = '👎' in new_emojis
        
        if had_dislike != has_dislike:
            if has_dislike:
                self._log_action(user_id, message_id, "поставил дизлайк", "👎")
            else:
                self._log_action(user_id, message_id, "убрал дизлайк", "👎")
        
        # Логируем другие реакции для отладки
        for emoji in new_emojis:
            if emoji not in ['👍', '👎']:
                logger.info(f"Другая реакция {emoji} от {user_id}")
    
    def _get_emoji(self, reaction: dict) -> str:
        """Извлекаем эмодзи из реакции"""
        if isinstance(reaction, dict):
            # Стандартная структура
            if 'emoji' in reaction:
                return reaction['emoji']
            # Альтернативная структура
            elif 'type' in reaction:
                rtype = reaction['type']
                if isinstance(rtype, dict):
                    return rtype.get('emoji', '')
                elif isinstance(rtype, str):
                    return rtype
        return ''
    
    def _log_action(self, user_id: int, message_id: int, action: str, emoji: str):
        """Логируем действие"""
        timestamp = datetime.now().strftime("%H:%M:%S")
        log_msg = f"[{timestamp}] User {user_id} {action} ({emoji}) на сообщение {message_id}"
        
        # В консоль
        logger.info(f"📝 {log_msg}")
        
        # В файл
        try:
            with open("reactions.log", "a", encoding="utf-8") as f:
                f.write(f"{log_msg}\n")
        except:
            pass
    
    async def test_bot(self):
        """Тестируем бота"""
        try:
            url = f"https://api.telegram.org/bot{self.bot_token}/getMe"
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as response:
                    if response.status == 200:
                        data = await response.json()
                        if data.get('ok'):
                            bot = data['result']
                            logger.info(f"✅ Бот: @{bot.get('username')}")
                            return True
            return False
        except Exception as e:
            logger.error(f"❌ Тест бота: {e}")
            return False


async def main_simple():
    """Упрощенный запуск"""
    # Настройка логирования - ТОЛЬКО В КОНСОЛЬ, подробно
    logging.basicConfig(
        level=logging.DEBUG,  # Меняем на DEBUG чтобы видеть ВСЕ
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    try:
        print("=" * 50)
        print("🚀 ЗАПУСК МОНИТОРИНГА РЕАКЦИЙ")
        print("=" * 50)
        
        monitor = ReactionMonitor()
        
        # Тест бота
        print("🔍 Проверяю бота...")
        if not await monitor.test_bot():
            print("❌ Бот не работает. Проверь токен в .env")
            return
        
        print("✅ Бот работает")
        print(f"👁️  Мониторю группу: {monitor.preview_group}")
        print("\n⚡ Поставь лайк (👍) или дизлайк (👎) в группе")
        print("⚡ И смотри вывод здесь")
        print("=" * 50 + "\n")
        
        # Запускаем
        await monitor.run_monitoring()
        
    except KeyboardInterrupt:
        print("\n\n⏹️  Остановлено")
    except Exception as e:
        print(f"\n❌ Ошибка: {e}")


if __name__ == "__main__":
    # Быстрая проверка .env
    if not os.getenv('PUBLISH_API'):
        print("❌ Ошибка: Создай файл .env с содержимым:")
        print("PUBLISH_API=8112892888:AAEeqmWZY0YPdUXAlLzm-TVC4KJIzMD_dZ8")
        print("PREVIEW_GROUP=-1001234567890")
        exit(1)
    
    asyncio.run(main_simple())