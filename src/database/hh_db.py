from typing import Any, Optional

import psycopg2
from psycopg2 import sql
from psycopg2.extensions import connection
from psycopg2.extras import execute_batch

from config import config
from src.logging_config import LoggingConfigClassMixin
from src.models.employer import Employer
from src.models.vacancy import Vacancy


class HeadHunterDataBase(LoggingConfigClassMixin):
    """Класс для создания базы данных с компаниями и вакансиями сайта HeadHunter.ru"""

    def __init__(self, dbname: str = "headhunter_vacancies") -> None:
        """Конструктор класса"""
        super().__init__()
        self._base_dbname: str = "postgres"
        self._hh_dbname = dbname
        self._params: dict = self._get_params()
        self._conn: Optional[connection] = None
        self.logger = self.configure()

    def __enter__(self) -> "HeadHunterDataBase":
        """Открывает соединение с базой данных"""
        try:
            self._conn = psycopg2.connect(**self._params, dbname=self.hh_dbname)
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
    def hh_dbname(self) -> str:
        """Возвращает название базы данных"""
        return self._hh_dbname

    @property
    def conn(self) -> Any:
        """Объект conn - соединения с базой данных"""
        if not self._conn:
            raise RuntimeError("DB connection not initialized")
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

    def create_database(self) -> None:
        """Создает базу данных для хранения данных о компаниях и вакансиях сайта HeadHunter.ru"""
        try:
            conn = psycopg2.connect(**self._params, dbname=self._base_dbname)
            conn.autocommit = True
            cur = conn.cursor()
            cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (self._hh_dbname,))
            is_exists = cur.fetchone()
            if is_exists:
                cur.execute(sql.SQL("DROP DATABASE {}").format(sql.Identifier(self.hh_dbname)))
                self.logger.info(f"База данных {self.hh_dbname} удалена")
            cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(self.hh_dbname)))
            self.logger.info(f"База данных {self.hh_dbname} создана")
        except psycopg2.Error as e:
            self.logger.error(f"Ошибка при создании базы данных: {e}")
            raise

    def create_table_hh_companies(self) -> None:
        """Создает таблицу для хранения данных о компаниях сайта HeadHunter.ru"""
        self._execute("""
            CREATE TABLE hh_companies (
                company_id SERIAL PRIMARY KEY,
                hh_employer_id VARCHAR UNIQUE,
                employer_name VARCHAR(255) NOT NULL,
                employer_url TEXT NOT NULL
                );
            """)
        self.conn.commit()
        self.logger.info("Tаблица hh_companies успешно создана")

    def create_table_hh_vacancies(self) -> None:
        """Создает таблицу для хранения данных о вакансиях компаний сайта HeadHunter.ru"""
        self._execute("""
            CREATE TABLE hh_vacancies (
                vacancy_id SERIAL PRIMARY KEY,
                hh_vac_id VARCHAR UNIQUE NOT NULL,
                vac_name VARCHAR(255) NOT NULL,
                vac_url TEXT NOT NULL,
                hh_employer_id VARCHAR,
                vac_area TEXT,
                salary_from INTEGER,
                salary_to INTEGER,
                average_salary NUMERIC GENERATED ALWAYS AS (
                    COALESCE((salary_from + salary_to) / 2, salary_from, salary_to)
                ) STORED,
                CONSTRAINT fk_hh_vacancies_hh_employer_id
                FOREIGN KEY(hh_employer_id)
                REFERENCES hh_companies(hh_employer_id)
                ON DELETE CASCADE
                );
            """)
        self._execute("""
            CREATE INDEX idx_vacancies_employer_id
            ON hh_vacancies (hh_employer_id);
            """)
        self.conn.commit()
        self.logger.info("Tаблица hh_vacancies успешно создана")

    def _batch_insert(self, query: str, data: list[tuple]) -> None:
        """Универсальная массовая вставка данных"""
        with self.conn:
            with self.conn.cursor() as cur:
                execute_batch(cur, query, data)

    def save_data_to_table_hh_companies(self, employers: dict) -> None:
        """Сохранение данных о компаниях сайта HeadHunter.ru в базу данных"""
        data = [(emp.employer_id, emp.name, emp.url) for emp in employers.values()]
        query = """
            INSERT INTO hh_companies (hh_employer_id, employer_name, employer_url)
            VALUES (%s, %s, %s)
            ON CONFLICT (hh_employer_id) DO NOTHING
        """
        self._batch_insert(query, data)
        self.logger.info(f"Добавлено {len(data)} компаний")

    def save_data_to_table_hh_vacancies(self, vacancies: list[Vacancy]) -> None:
        """Сохранение данных о вакансиях компаний сайта HeadHunter.ru в базу данных"""
        data = [
            (vac.vac_id, vac.name, vac.url, vac.employer_id, vac.area, vac.salary_from, vac.salary_to)
            for vac in vacancies
        ]
        query = """
            INSERT INTO hh_vacancies
            (hh_vac_id, vac_name, vac_url, hh_employer_id, vac_area, salary_from, salary_to)
            VALUES (%s, %s, %s, %s, %s, COALESCE(%s,0), COALESCE(%s,0))
            ON CONFLICT (hh_vac_id) DO NOTHING
        """
        self._batch_insert(query, data)
        self.logger.info(f"Добавлено {len(data)} вакансий")

    @staticmethod
    def _get_params() -> dict:
        """Возвращает параметры подключения к базе данных"""
        return config()
