import asyncio
import logging
import json
import os
import time
from typing import Dict, Any, Optional
import aiohttp
from dotenv import load_dotenv

DEEPSEEK_URL = "https://api.deepseek.com/v1/chat/completions"

# Настроим логгирование
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO) 

# 1. Загрузка переменных окружения из файла .env
load_dotenv()

# 2. Получение ключа и его проверка
DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY")

if not DEEPSEEK_API_KEY:
    logger.critical("API-ключ Deepseek отсутствует. Невозможно выполнить запрос. Установите переменную окружения DEEPSEEK_API_KEY.")
    raise ValueError("Deepseek API key is not configured.")

# Настройки для экспоненциальной задержки
MAX_RETRIES = 3
BASE_DELAY = 1.0 # Задержка в секундах

async def call_deepseek_api(
    prompt: str, 
    text: str, 
    response_schema: Dict[str, Any],
    model_type: str,
    temperature: float = 0.7,
    tokens: int = 500,
    # НОВЫЙ ПАРАМЕТР: Позволяет отключить проверку SSL-сертификата (полезно для прокси/корпоративных сетей)
    verify_ssl: bool = True,
) -> Optional[Dict[str, Any]]:
    """
    Выполняет асинхронный HTTP-запрос к API Deepseek с повторными попытками.
    Использует режим tools/functions для гарантированного получения JSON-ответа.

    Аргументы:
        prompt (str): Системный промпт с инструкциями для анализа.
        text (str): Текст сообщения, который необходимо проанализировать.
        response_schema (Dict): JSON-схема для принудительного структурированного ответа.
        verify_ssl (bool): Включить/отключить проверку SSL-сертификата. По умолчанию True.

    Возвращает:
        Optional[Dict]: Распарсенный JSON-ответ от ИИ или None в случае критической ошибки.
    """

    if not DEEPSEEK_API_KEY:
        logging.critical("API-ключ Deepseek отсутствует. Невозможно выполнить запрос.")
        return None

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {DEEPSEEK_API_KEY}"
    }

    # Формируем тело запроса
    payload = {
        "model": model_type,
        # Запрос состоит из двух частей: системный промпт и пользовательский текст.
        "messages": [
            {"role": "system", "content": prompt},
            {"role": "user", "content": f"Анализируемый текст: ```{text}```"}
        ],
        # Настройки для гарантированного JSON-ответа
        "tool_choice": {"type": "function", "function": {"name": "analyze_message"}},
        "tools": [
            {
                "type": "function",
                "function": {
                    "name": "analyze_message",
                    "description": "Анализирует текст на соответствие критериям фильтрации.",
                    "parameters": response_schema
                }
            }
        ],
        "temperature": temperature,
        "max_tokens": tokens
    }

    # Создаем TCPConnector, чтобы явно указать, нужно ли проверять SSL
    connector = aiohttp.TCPConnector(ssl=verify_ssl)

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            # Используем ClientSession с настроенным коннектором
            async with aiohttp.ClientSession(headers=headers, connector=connector) as session:
                async with session.post(DEEPSEEK_URL, json=payload) as response:
                    
                    if response.status == 200:
                        data = await response.json()
                        
                        # Проверяем, что ответ содержит вызов функции (tool_calls)
                        if (
                            data.get('choices') and 
                            data['choices'][0].get('message') and 
                            data['choices'][0]['message'].get('tool_calls')
                        ):
                            # Извлекаем аргументы функции, которые являются нашим JSON-ответом
                            function_args_str = data['choices'][0]['message']['tool_calls'][0]['function']['arguments']
                            
                            # Парсим строку аргументов в Python-словарь
                            return json.loads(function_args_str)
                        else:
                            logging.error(f"Deepseek: Попытка {attempt}: Ответ 200 OK, но не содержит ожидаемого tool_calls.")
                            raise ValueError("Неструктурированный ответ от ИИ.")
                    
                    # Обработка ошибок, которые могут быть временными (429, 500, 503)
                    elif response.status in [429, 500, 503]:
                        logging.warning(f"Deepseek: Попытка {attempt}: Ошибка {response.status}. Текст: {await response.text()}")
                        if attempt < MAX_RETRIES:
                            # Экспоненциальный откат
                            delay = 2 ** (attempt - 1)
                            logging.info(f"Deepseek: Ожидание {delay} с. перед повторной попыткой.")
                            await asyncio.sleep(delay)
                        continue # Переход к следующей попытке
                    
                    # Критическая ошибка (например, 401 Unauthorized, 400 Bad Request)
                    else:
                        logging.error(f"Deepseek: Критическая ошибка {response.status}. Текст: {await response.text()}")
                        return None
        
        except (aiohttp.ClientConnectorError, asyncio.TimeoutError) as e:
            # Ошибки сети или таймаут, включая ошибку SSL
            logging.warning(f"Deepseek: Попытка {attempt}: Ошибка подключения/таймаут: {e}")
            if attempt < MAX_RETRIES:
                delay = 2 ** (attempt - 1)
                logging.info(f"Deepseek: Ожидание {delay} с. перед повторной попыткой.")
                await asyncio.sleep(delay)
            continue
        
        except Exception as e:
            # Другие ошибки (например, проблемы с парсингом JSON)
            logging.error(f"Deepseek: Непредвиденная ошибка в цикле запроса: {e}")
            return None # Прекращаем выполнение

    logging.error(f"Deepseek: Не удалось получить ответ после {MAX_RETRIES} попыток.")
    return None
