# database/database.py
import asyncpg
import logging
from typing import Optional
from database.database_config import DatabaseConfig

class Database:
    """
    Единый менеджер подключений к БД для всех служб.
    """
    _pool: Optional[asyncpg.Pool] = None
    
    @classmethod
    async def get_pool(cls) -> asyncpg.Pool:
        """
        Возвращает пул подключений к БД.
        """
        if cls._pool is None:
            logging.info("Создание пула подключений к БД...")
            try:
                # Логируем параметры подключения (без пароля)
                logging.info(f"Параметры подключения к БД:")
                logging.info(f"  Хост: {DatabaseConfig.DB_HOST}")
                logging.info(f"  Порт: {DatabaseConfig.DB_PORT}")
                logging.info(f"  База данных: {DatabaseConfig.DB_NAME}")
                logging.info(f"  Пользователь: {DatabaseConfig.DB_USER}")
                logging.info(f"  SSL: require")
                logging.info(f"  Размер пула: min=2, max=8")
                
                cls._pool = await asyncpg.create_pool(
                    user=DatabaseConfig.DB_USER,
                    password=DatabaseConfig.DB_PASS,
                    database=DatabaseConfig.DB_NAME,
                    host=DatabaseConfig.DB_HOST,
                    port=DatabaseConfig.DB_PORT,
                    ssl='require',
                    min_size=2,
                    max_size=8,
                    max_inactive_connection_lifetime=60
                )
                
                # Проверяем подключение
                async with cls._pool.acquire() as test_conn:
                    db_version = await test_conn.fetchval("SELECT version();")
                    logging.info(f"✅ Пул подключений к БД создан успешно")
                    logging.info(f"   Версия БД: {db_version.split(',')[0]}")
                    
            except Exception as e:
                logging.critical(f"❌ Критическая ошибка создания пула БД: {e}")
                logging.critical(f"   Проверьте параметры подключения в database_config.py")
                logging.critical(f"   Хост: {DatabaseConfig.DB_HOST}:{DatabaseConfig.DB_PORT}")
                logging.critical(f"   База: {DatabaseConfig.DB_NAME}, Пользователь: {DatabaseConfig.DB_USER}")
                raise
        
        return cls._pool
    
    @classmethod
    async def initialize_database(cls):
        """
        Инициализация подключения к БД.
        Должна вызываться ОДИН раз при запуске приложения.
        """
        logging.info("🔄 Инициализация подключения к БД...")
        
        try:
            pool = await cls.get_pool()
            logging.info("✅ Получен пул подключений для инициализации БД")
            
            # Просто проверяем, что подключение работает
            async with pool.acquire() as conn:
                db_name = await conn.fetchval("SELECT current_database();")
                logging.info(f"✅ Подключено к БД: {db_name}")
                
        except Exception as e:
            logging.critical(f"❌ Не удалось инициализировать подключение к БД: {e}")
            raise
    
    @classmethod
    async def test_connection(cls):
        """
        Тестирует подключение к БД и возвращает статус.
        """
        try:
            pool = await cls.get_pool()
            async with pool.acquire() as conn:
                # Выполняем простой запрос для проверки подключения
                result = await conn.fetchval("SELECT 1")
                if result == 1:
                    logging.info("✅ Тест подключения к БД: УСПЕХ")
                    return True
                else:
                    logging.error("❌ Тест подключения к БД: НЕИЗВЕСТНАЯ ОШИБКА")
                    return False
        except Exception as e:
            logging.critical(f"❌ Тест подключения к БД: ОШИБКА - {e}")
            return False
    
    @classmethod
    async def close(cls):
        """
        Закрывает пул подключений.
        Вызывается при завершении приложения.
        """
        logging.info("🔌 Завершение работы с БД...")
        
        if cls._pool:
            await cls._pool.close()
            cls._pool = None
            logging.info("✅ Пул подключений к БД закрыт")