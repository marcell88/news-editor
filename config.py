# config.py
import os
from dotenv import load_dotenv

# Загружаем переменные окружения
load_dotenv()

class Config:
    """Конфигурация приложения"""
    
    # Telegram API
    TELEGRAM_BOT_TOKEN = os.getenv('PUBLISH_API')
    TELEGRAM_GROUP = os.getenv('TG_GROUP')
    
    # Проверяем наличие обязательных переменных
    @classmethod
    def validate(cls):
        """Проверяет наличие обязательных переменных окружения"""
        missing = []
        
        if not cls.TELEGRAM_BOT_TOKEN:
            missing.append('PUBLISH_API (Telegram Bot Token)')
        
        if not cls.TELEGRAM_GROUP:
            missing.append('TG_GROUP (Telegram Group ID/Username)')
        
        if missing:
            raise ValueError(f"Отсутствуют обязательные переменные окружения: {', '.join(missing)}")
        
        return True