from typing import Any, Optional

import psycopg2
from psycopg2.extensions import connection
from psycopg2.extras import execute_batch

from config import get_db_params, get_db_name
from src.logging_config import LoggingConfigClassMixin
from src.models.vacancy import Vacancy


class HabrCareerDBWriter(LoggingConfigClassMixin):
    """Класс для заполнения базы данных HabrCareer вакансиями и работодателями"""

    def __init__(self) -> None:
        super().__init__()
        self._hc_dbname = get_db_name()
        self._params: dict = self._get_params()
        self._conn: Optional[connection] = None
        self.logger = self.configure()

    def __enter__(self) -> 'HabrCareerDBWriter':
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
        except psycopg2.Error as e:
            self.conn.rollback()
            self.logger.error(f"Ошибка при работе с базой данных: {e}")
            raise

    def _batch_insert(self, query: str, data: list[tuple]) -> None:
        """Универсальная массовая вставка данных"""
        if not data:
            return
        with self.conn:
            with self.conn.cursor() as cur:
                execute_batch(cur, query, data)

    def save_data_to_table_hc_companies(self, employers: dict) -> None:
        """Сохранение данных о компаниях в базу данных"""
        data = [(emp.employer_id, emp.name, emp.url) for emp in employers.values()]
        query = """
            INSERT INTO hc_companies (hc_employer_id, employer_name, employer_url)
            VALUES (%s, %s, %s)
            ON CONFLICT (hc_employer_id) DO NOTHING
        """
        self._batch_insert(query, data)
        self.logger.info(f"Добавлено {len(data)} компаний")

    def save_data_to_table_hc_vacancies(self, vacancies: list[Vacancy]) -> None:
        """Сохранение данных о вакансиях в базу данных"""
        data = [
            (vac.vac_id, vac.name, vac.url, vac.employer_id, vac.area, vac.salary_from, vac.salary_to)
            for vac in vacancies
        ]
        query = """
            INSERT INTO hc_vacancies
            (hc_vac_id, vac_name, vac_url, hc_employer_id, vac_area, salary_from, salary_to)
            VALUES (%s, %s, %s, %s, %s, COALESCE(%s,0), COALESCE(%s,0))
            ON CONFLICT (hc_vac_id) DO NOTHING
        """
        self._batch_insert(query, data)
        self.logger.info(f"Добавлено {len(data)} вакансий")

    @staticmethod
    def _get_params() -> dict:
        """Возвращает параметры подключения к базе данных"""
        return get_db_params()
