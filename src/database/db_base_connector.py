from typing import Any, Optional

import psycopg2
from psycopg2.extensions import connection

from config import get_db_name, get_db_params
from src.logging_config import LoggingConfigClassMixin


class DataBaseConnector(LoggingConfigClassMixin):
    """Класс для открытия, закрытия соединения с базой данных"""

    def __init__(self) -> None:
        super().__init__()
        self._hc_dbname = get_db_name()
        self._params: dict = get_db_params()
        self._conn: Optional[connection] = None
        self.logger = self.configure()

    def __enter__(self):
        """Открывает соединение с базой данных"""
        try:
            self._conn = psycopg2.connect(**self._params, dbname=self.hc_dbname)
            self._conn.autocommit = False
            self.logger.info("Соединение с базой данных открыто")
            return self
        except psycopg2.Error as e:
            self.logger.error(f"Ошибка подключения к базе данных: {e}")
            raise

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Закрывает соединение с базой данных"""
        if self._conn is not None:
            self._conn.close()
            self.logger.info("Соединение с базой данных закрыто")

    @property
    def hc_dbname(self) -> str:
        """Возвращает название базы данных"""
        return self._hc_dbname

    @property
    def conn(self) -> Any:
        """Объект conn - соединения с базой данных"""
        if not self._conn:
            raise RuntimeError("Соединение с базой данных не установлено")
        return self._conn

    def _execute(self, query: str, params: Optional[tuple] = None, fetch: bool = False) -> Any:
        """Вспомогательный метод для выполнения SQL-запросов"""
        try:
            with self.conn.cursor() as cur:
                cur.execute(query, params)
                if fetch:
                    return cur.fetchall()
            self.conn.commit()
        except psycopg2.Error as e:
            if self._conn:
                self._conn.rollback()
            self.logger.error(f"Ошибка при работе с базой данных: {e}")
            raise
