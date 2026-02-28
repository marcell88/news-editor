# app.py (полная версия со всеми службами)
import asyncio
import logging
import signal
from typing import Dict, Any

from database.database import Database
from services.lt_state_updater import LTStateUpdater
from services.lt_editor_monitor import LTEditorMonitor
from services.mt_balancer import MTBalancer
from services.timer import TimerService
from services.calculator import CalculatorService
from services.planner import PlannerService
from services.painter import PainterService
from services.preparator import PreparatorService
from services.previewer import PreviewerService
from services.reaction_monitor import ReactionMonitor
from services.publisher import PublisherService
from services.cleaner import CleanerService
from services.st_catcher import STCatcher  # НОВАЯ СЛУЖБА

# Настраиваем логирование для точки входа.
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# === НАСТРОЙКА ВКЛЮЧЕНИЯ СЛУЖБ ===
ENABLE_LT_SERVICES = True      # LT-службы (долгосрочные)
ENABLE_MT_BALANCER = False     # MT-Balancer (среднесрочные) - теперь через Planner
ENABLE_TIMER_SERVICE = False   # Timer Service (временные оценки) - теперь через Planner
ENABLE_CALCULATOR = True       # Calculator Service (итоговые оценки) - постоянно
ENABLE_PLANNER = True          # Planner Service (планировщик) - постоянно
ENABLE_PAINTER = True          # Painter Service (генерация изображений) - постоянно
ENABLE_PREPARATOR = True       # Preparator Service (подготовка текстов) - постоянно
ENABLE_PREVIEWER = True        # Previewer Service (публикация в preview группу)
ENABLE_REACTION_MONITOR = False  # Reaction Monitor (мониторинг реакций)
ENABLE_PUBLISHER = True        # Publisher Service (прямая публикация в группу)
ENABLE_CLEANER = True          # Cleaner Service (очистка БД)
ENABLE_ST_CATCHER = True       # ST-Catcher служба (краткосрочный анализ) - НОВАЯ
# =================================

class ServiceManager:
    """Менеджер для управления всеми службами."""
    
    def __init__(self):
        self.tasks = []
        self.is_running = True
        
        # Инициализируем все службы
        self.services: Dict[str, Any] = {}
        
        if ENABLE_LT_SERVICES:
            self.services["lt_state_updater"] = LTStateUpdater()
            self.services["lt_editor_monitor"] = LTEditorMonitor()
        
        if ENABLE_MT_BALANCER:
            self.services["mt_balancer"] = MTBalancer()
        
        if ENABLE_TIMER_SERVICE:
            from datetime import datetime
            target_hour = datetime.now().hour
            self.services["timer_service"] = TimerService(target_hour=target_hour)
            logging.info(f"⏰ Timer Service инициализирован для часа {target_hour}")
        
        if ENABLE_CALCULATOR:
            self.services["calculator_service"] = CalculatorService()
        
        if ENABLE_PLANNER:
            self.services["planner_service"] = PlannerService()
        
        if ENABLE_PAINTER:
            self.services["painter_service"] = PainterService()
        
        if ENABLE_PREPARATOR:
            self.services["preparator_service"] = PreparatorService()
        
        if ENABLE_PREVIEWER:
            self.services["previewer_service"] = PreviewerService()
        
        if ENABLE_REACTION_MONITOR:
            self.services["reaction_monitor"] = ReactionMonitor()
        
        if ENABLE_PUBLISHER:
            self.services["publisher_service"] = PublisherService()
        
        if ENABLE_CLEANER:
            self.services["cleaner_service"] = CleanerService()
        
        if ENABLE_ST_CATCHER:  # НОВАЯ СЛУЖБА
            self.services["st_catcher"] = STCatcher()
        
        # Обработка сигналов остановки
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum, frame):
        """Обработчик сигналов остановки."""
        logging.info(f"Получен сигнал остановки {signum}")
        self.is_running = False

    async def initialize_services(self):
        """Инициализация всех служб перед запуском."""
        try:
            logging.info("🔄 Инициализация базы данных...")
            await Database.initialize_database()
            logging.info("✅ База данных инициализирована")
            
            return True
        except Exception as e:
            logging.critical(f"❌ Ошибка инициализации БД: {e}")
            return False

    async def start_background_services(self):
        """Запускает все фоновые службы."""
        services_tasks = []
        
        # Создаем задачи для фоновых служб
        if ENABLE_LT_SERVICES and "lt_state_updater" in self.services:
            services_tasks.append(("LT-State-Updater", self._run_lt_state_updater))
        
        if ENABLE_LT_SERVICES and "lt_editor_monitor" in self.services:
            services_tasks.append(("LT-Editor-Monitor", self._run_lt_editor_monitor))
        
        if ENABLE_CALCULATOR and "calculator_service" in self.services:
            services_tasks.append(("Calculator-Service", self._run_calculator_service))
        
        if ENABLE_PLANNER and "planner_service" in self.services:
            services_tasks.append(("Planner-Service", self._run_planner_service))
        
        if ENABLE_PAINTER and "painter_service" in self.services:
            services_tasks.append(("Painter-Service", self._run_painter_service))
        
        if ENABLE_PREPARATOR and "preparator_service" in self.services:
            services_tasks.append(("Preparator-Service", self._run_preparator_service))
        
        if ENABLE_PREVIEWER and "previewer_service" in self.services:
            services_tasks.append(("Previewer-Service", self._run_previewer))
        
        if ENABLE_REACTION_MONITOR and "reaction_monitor" in self.services:
            services_tasks.append(("Reaction-Monitor", self._run_reaction_monitor))
        
        if ENABLE_PUBLISHER and "publisher_service" in self.services:
            services_tasks.append(("Publisher-Service", self._run_publisher_service))
        
        if ENABLE_CLEANER and "cleaner_service" in self.services:
            services_tasks.append(("Cleaner-Service", self._run_cleaner_service))
        
        if ENABLE_ST_CATCHER and "st_catcher" in self.services:  # НОВАЯ СЛУЖБА
            services_tasks.append(("ST-Catcher", self._run_st_catcher))
        
        if not services_tasks:
            logging.warning("⚠️ Нет активных фоновых служб.")
            return False
        
        logging.info("🎯 Запуск фоновых служб...")
        for name, service_task in services_tasks:
            task = asyncio.create_task(service_task(name))
            self.tasks.append(task)
            await asyncio.sleep(0.5)
        
        return True

    async def _run_lt_state_updater(self, name: str):
        """Запускает службу обновления state."""
        try:
            logging.info(f"🚀 Запуск службы {name}...")
            await self.services["lt_state_updater"].run_periodic_check()
        except asyncio.CancelledError:
            logging.info(f"Служба {name} остановлена")
        except Exception as e:
            logging.error(f"❌ Ошибка в службе {name}: {e}")

    async def _run_lt_editor_monitor(self, name: str):
        """Запускает службу мониторинга editor."""
        try:
            logging.info(f"🚀 Запуск службы {name}...")
            await self.services["lt_editor_monitor"].run_monitoring()
        except asyncio.CancelledError:
            logging.info(f"Служба {name} остановлена")
        except Exception as e:
            logging.error(f"❌ Ошибка в службе {name}: {e}")

    async def _run_calculator_service(self, name: str):
        """Запускает Calculator Service."""
        try:
            logging.info(f"🚀 Запуск службы {name}...")
            await self.services["calculator_service"].run_monitoring()
        except asyncio.CancelledError:
            logging.info(f"Служба {name} остановлена")
        except Exception as e:
            logging.error(f"❌ Ошибка в службе {name}: {e}")

    async def _run_planner_service(self, name: str):
        """Запускает Planner Service."""
        try:
            logging.info(f"🚀 Запуск службы {name}...")
            await self.services["planner_service"].run_monitoring()
        except asyncio.CancelledError:
            logging.info(f"Служба {name} остановлена")
        except Exception as e:
            logging.error(f"❌ Ошибка в службе {name}: {e}")

    async def _run_painter_service(self, name: str):
        """Запускает Painter Service."""
        try:
            logging.info(f"🚀 Запуск службы {name}...")
            await self.services["painter_service"].run_monitoring()
        except asyncio.CancelledError:
            logging.info(f"Служба {name} остановлена")
        except Exception as e:
            logging.error(f"❌ Ошибка в службе {name}: {e}")

    async def _run_preparator_service(self, name: str):
        """Запускает Preparator Service."""
        try:
            logging.info(f"🚀 Запуск службы {name}...")
            await self.services["preparator_service"].run_monitoring()
        except asyncio.CancelledError:
            logging.info(f"Служба {name} остановлена")
        except Exception as e:
            logging.error(f"❌ Ошибка в службе {name}: {e}")

    async def _run_previewer(self, name: str):
        """Запускает Previewer Service."""
        try:
            logging.info(f"🚀 Запуск службы {name}...")
            await self.services["previewer_service"].run_monitoring()
        except asyncio.CancelledError:
            logging.info(f"Служба {name} остановлена")
        except Exception as e:
            logging.error(f"❌ Ошибка в службе {name}: {e}")

    async def _run_reaction_monitor(self, name: str):
        """Запускает Reaction Monitor Service."""
        try:
            logging.info(f"🚀 Запуск службы {name}...")
            await self.services["reaction_monitor"].run_monitoring()
        except asyncio.CancelledError:
            logging.info(f"Служба {name} остановлена")
        except Exception as e:
            logging.error(f"❌ Ошибка в службе {name}: {e}")

    async def _run_publisher_service(self, name: str):
        """Запускает Publisher Service."""
        try:
            logging.info(f"🚀 Запуск службы {name}...")
            await self.services["publisher_service"].run_monitoring()
        except asyncio.CancelledError:
            logging.info(f"Служба {name} остановлена")
        except Exception as e:
            logging.error(f"❌ Ошибка в службе {name}: {e}")

    async def _run_cleaner_service(self, name: str):
        """Запускает Cleaner Service."""
        try:
            logging.info(f"🚀 Запуск службы {name}...")
            await self.services["cleaner_service"].run_monitoring()
        except asyncio.CancelledError:
            logging.info(f"Служба {name} остановлена")
        except Exception as e:
            logging.error(f"❌ Ошибка в службе {name}: {e}")

    async def _run_st_catcher(self, name: str):  # НОВАЯ СЛУЖБА
        """Запускает ST-Catcher службу."""
        try:
            logging.info(f"🚀 Запуск службы {name}...")
            await self.services["st_catcher"].run_monitoring()
        except asyncio.CancelledError:
            logging.info(f"Служба {name} остановлена")
        except Exception as e:
            logging.error(f"❌ Ошибка в службе {name}: {e}")

    async def stop_services(self):
        """Останавливает все службы."""
        logging.info("🛑 Остановка всех служб...")
        
        # Отменяем все задачи
        for task in self.tasks:
            if not task.done():
                task.cancel()
        
        # Ждем завершения задач
        if self.tasks:
            try:
                await asyncio.gather(*self.tasks, return_exceptions=True)
            except Exception as e:
                logging.debug(f"Исключение при остановке задач: {e}")
        
        # Закрываем соединения с БД
        try:
            await Database.close()
            logging.info("✅ Соединения с БД закрыты")
        except Exception as e:
            logging.error(f"❌ Ошибка при закрытии БД: {e}")

    async def run(self):
        """Основной цикл работы."""
        try:
            # Инициализация перед запуском служб
            if not await self.initialize_services():
                logging.critical("❌ Не удалось инициализировать БД. Завершение работы.")
                return
            
            # Логируем статус включения служб
            self._log_services_status()
            
            # Запускаем фоновые службы
            has_background_services = await self.start_background_services()
            
            if has_background_services:
                logging.info("🏃 Приложение работает (фоновые службы активны)")
                while self.is_running:
                    await asyncio.sleep(1)
            else:
                logging.info("⏳ Нет фоновых служб, приложение завершит работу через 5 секунд...")
                await asyncio.sleep(5)
                self.is_running = False
            
            # Статистика работы
            if self.tasks:
                self._log_tasks_statistics()
                
        except KeyboardInterrupt:
            logging.info("Получен KeyboardInterrupt")
        except Exception as e:
            logging.critical(f"Непредвиденная ошибка в app.py: {e}")
        finally:
            await self.stop_services()
    
    def _log_services_status(self):
        """Логирует статус всех служб."""
        logging.info("📋 Статус служб:")
        logging.info("  🔁 Фоновые службы:")
        logging.info(f"    LT-State-Updater: {'✅ ВКЛЮЧЕН' if ENABLE_LT_SERVICES else '❌ ВЫКЛЮЧЕН'}")
        logging.info(f"    LT-Editor-Monitor: {'✅ ВКЛЮЧЕН' if ENABLE_LT_SERVICES else '❌ ВЫКЛЮЧЕН'}")
        logging.info(f"    Calculator Service: {'✅ ВКЛЮЧЕН' if ENABLE_CALCULATOR else '❌ ВЫКЛЮЧЕН'}")
        logging.info(f"    Planner Service: {'✅ ВКЛЮЧЕН' if ENABLE_PLANNER else '❌ ВЫКЛЮЧЕН'}")
        logging.info(f"    Painter Service: {'✅ ВКЛЮЧЕН' if ENABLE_PAINTER else '❌ ВЫКЛЮЧЕН'}")
        logging.info(f"    Preparator Service: {'✅ ВКЛЮЧЕН' if ENABLE_PREPARATOR else '❌ ВЫКЛЮЧЕН'}")
        logging.info(f"    Previewer Service: {'✅ ВКЛЮЧЕН' if ENABLE_PREVIEWER else '❌ ВЫКЛЮЧЕН'}")
        logging.info(f"    Reaction Monitor: {'✅ ВКЛЮЧЕН' if ENABLE_REACTION_MONITOR else '❌ ВЫКЛЮЧЕН'}")
        logging.info(f"    Publisher Service: {'✅ ВКЛЮЧЕН' if ENABLE_PUBLISHER else '❌ ВЫКЛЮЧЕН'}")
        logging.info(f"    Cleaner Service: {'✅ ВКЛЮЧЕН' if ENABLE_CLEANER else '❌ ВЫКЛЮЧЕН'}")
        logging.info(f"    ST-Catcher Service: {'✅ ВКЛЮЧЕН' if ENABLE_ST_CATCHER else '❌ ВЫКЛЮЧЕН'}")  # НОВАЯ СЛУЖБА
        logging.info("=" * 60)
    
    def _log_tasks_statistics(self):
        """Логирует статистику по задачам."""
        logging.info("📊 Статистика работы служб:")
        for i, task in enumerate(self.tasks):
            if task.done():
                if task.cancelled():
                    status = "отменена"
                else:
                    try:
                        task.result()
                        status = "завершена успешно"
                    except Exception as e:
                        status = f"завершена с ошибкой: {e}"
            else:
                status = "работает"
            logging.info(f"  Задача {i+1}: {status}")

async def main_services():
    """Запускает все службы через менеджер."""
    manager = ServiceManager()
    await manager.run()

def start_application():
    """Основная функция для запуска приложения."""
    logging.info("🚀 Запуск приложения Balancer Services...")
    logging.info("=" * 60)
    logging.info("📋 Настройки запуска:")
    logging.info("  🔁 Фоновые службы:")
    logging.info(f"    LT-службы: {'✅ ВКЛЮЧЕНЫ' if ENABLE_LT_SERVICES else '❌ ВЫКЛЮЧЕНЫ'}")
    logging.info(f"    Calculator: {'✅ ВКЛЮЧЕН' if ENABLE_CALCULATOR else '❌ ВЫКЛЮЧЕН'}")
    logging.info(f"    Planner: {'✅ ВКЛЮЧЕН' if ENABLE_PLANNER else '❌ ВЫКЛЮЧЕН'}")
    logging.info(f"    Painter: {'✅ ВКЛЮЧЕН' if ENABLE_PAINTER else '❌ ВЫКЛЮЧЕН'}")
    logging.info(f"    Preparator: {'✅ ВКЛЮЧЕН' if ENABLE_PREPARATOR else '❌ ВЫКЛЮЧЕН'}")
    logging.info(f"    Previewer: {'✅ ВКЛЮЧЕН' if ENABLE_PREVIEWER else '❌ ВЫКЛЮЧЕН'}")
    logging.info(f"    Reaction Monitor: {'✅ ВКЛЮЧЕН' if ENABLE_REACTION_MONITOR else '❌ ВЫКЛЮЧЕН'}")
    logging.info(f"    Publisher: {'✅ ВКЛЮЧЕН' if ENABLE_PUBLISHER else '❌ ВЫКЛЮЧЕН'}")
    logging.info(f"    Cleaner: {'✅ ВКЛЮЧЕН' if ENABLE_CLEANER else '❌ ВЫКЛЮЧЕН'}")
    logging.info(f"    ST-Catcher: {'✅ ВКЛЮЧЕН' if ENABLE_ST_CATCHER else '❌ ВЫКЛЮЧЕН'}")  # НОВАЯ СЛУЖБА
    logging.info("=" * 60)
    
    # Проверяем, что хотя бы одна служба включена
    enabled_services = [
        ENABLE_LT_SERVICES,
        ENABLE_CALCULATOR,
        ENABLE_PLANNER,
        ENABLE_PAINTER,
        ENABLE_PREPARATOR,
        ENABLE_PREVIEWER,
        ENABLE_REACTION_MONITOR,
        ENABLE_PUBLISHER,
        ENABLE_CLEANER,
        ENABLE_ST_CATCHER  # ДОБАВЛЕНО
    ]
    
    if not any(enabled_services):
        logging.warning("⚠️ Все службы отключены! Запуск не имеет смысла.")
        return
    
    try:
        asyncio.run(main_services())
    except KeyboardInterrupt:
        logging.info("Приложение остановлено пользователем (Ctrl+C).")
    except Exception as e:
        logging.critical(f"Непредвиденная ошибка в app.py: {e}")
    finally:
        logging.info("Приложение завершило работу.")

if __name__ == '__main__':
    start_application()