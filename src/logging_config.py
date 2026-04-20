import inspect
import logging
import os
from pathlib import Path
from typing import Literal, Optional

LogLevel = int | Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]


class LoggingConfigClassMixin:
    """
    Миксин для настройки логеров. Дает возможность настраивать логер с выводом в файл и/или консоль.
    :name: Имя логгера (обычно __name__)
    :level: уровень логирования (по умолчанию INFO)
    :log_file: путь к лог-файлу (если нужно логировать в файл)
    :log_to_console: по умолчанию не создается (если нужно логировать в консоль)
    :fmt: формат сообщения,
    :clear_log_on_start: удаляет старый лог-файл один раз при запуске,
    """

    def __init__(
        self,
        name: Optional[str] = None,
        level: LogLevel = "INFO",
        log_file: Optional[str] = None,
        log_to_console: bool = False,
        clear_log_on_start: bool = True,
        fmt: str = "%(asctime)s - %(levelname)s - logger:%(name)s - module:%(module)s "
        "- func:%(funcName)s:%(lineno)d - %(message)s",
    ) -> None:
        """Конструктор для класса логгирования"""
        self.name = name or self._get_caller_module_name()
        self.level = level
        self.log_file = log_file or f"{self.name}.log"
        self.log_to_console = log_to_console
        self.clear_log_on_start = clear_log_on_start
        self.fmt = fmt

    def configure(self) -> logging.Logger:
        """Возвращает экземпляр логгера с заданной конфигурацией"""
        logger = logging.getLogger(self.name)

        if "AIRFLOW_CTX_DAG_ID" in os.environ:
            return logging.getLogger("airflow.task")

        logger.setLevel(self.level)

        if logger.hasHandlers():
            return logger

        formatter = logging.Formatter(self.fmt, datefmt="%Y-%m-%d %H:%M:%S")

        if self.log_to_console:
            self._add_console_handler(logger, formatter)

        if self.log_file:
            self._add_file_handler(logger, formatter)

        return logger

    def _add_file_handler(self, logger: logging.Logger, formatter: logging.Formatter) -> None:
        """Создает Хэндлер для записи логов в файл"""
        logs_dir = Path(__file__).resolve().parent.parent / "logs"
        logs_dir.mkdir(parents=True, exist_ok=True)
        log_path = logs_dir / self.log_file

        if self.clear_log_on_start and log_path.exists():
            log_path.unlink()

        file_handler = logging.FileHandler(log_path, mode="a", encoding="utf-8")
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    @staticmethod
    def _add_console_handler(logger: logging.Logger, formatter: logging.Formatter) -> None:
        """Создает Хэндлер для вывода логов в консоль"""
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    @staticmethod
    def _get_caller_module_name() -> str:
        """Возвращает имя модуля, вызвавшего конфигурацию логгера"""
        caller_frame = inspect.stack()[2]
        caller_module = inspect.getmodule(caller_frame.frame)
        return caller_module.__name__ if caller_module else "unknown"
