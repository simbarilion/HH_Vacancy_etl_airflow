from typing import Any, Optional

import psycopg2
from psycopg2 import sql
from psycopg2.extensions import connection
from psycopg2.extras import execute_batch

from config import get_db_params, get_db_base_name, get_db_name
from src.logging_config import LoggingConfigClassMixin
from src.models.vacancy import Vacancy


class HabrCareerDataBase(LoggingConfigClassMixin):
    """Класс для создания базы данных HabrCareer (вакансии и работодатели)"""

    def __init__(self, dbname: str) -> None:
        super().__init__()
        self._base_dbname: str = get_db_base_name()
        self._hc_dbname = dbname or get_db_name()
        self._params: dict = self._get_params()
        self._conn: Optional[connection] = None
        self.logger = self.configure()

    def __enter__(self) -> "HabrCareerDataBase":
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

    def create_database(self) -> None:
        """Создаёт базу данных, если она ещё не существует"""
        conn = psycopg2.connect(**self._params, dbname=self._base_dbname)
        conn.autocommit = True
        try:
            cur = conn.cursor()
            cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (self._hc_dbname,))
            if not cur.fetchone():
                cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(self.hc_dbname)))
                self.logger.info(f"База данных '{self.hc_dbname}' успешно создана")
            else:
                self.logger.info(f"База данных '{self._hc_dbname}' уже существует")
        except psycopg2.Error as e:
            self.logger.error(f"Ошибка при создании базы данных: {e}")
            raise
        finally:
            if "conn" in locals():
                conn.close()

    def prepare_tables(self) -> None:
        """Создаёт таблицы (если их нет) и очищает их от старых данных"""
        self._create_tables_if_not_exists()
        self._truncate_tables()
        self.logger.info("Таблицы подготовлены (созданы/очищены)")

    def _create_tables_if_not_exists(self) -> None:
        """Создаёт таблицы только если они не существуют"""

        self._execute("""
            CREATE TABLE IF NOT EXISTS hc_companies (
                company_id SERIAL PRIMARY KEY,
                hc_employer_id VARCHAR UNIQUE NOT NULL,
                employer_name VARCHAR(255) NOT NULL,
                employer_url TEXT NOT NULL
                );
            """)
        self.logger.info("Tаблица hc_companies проверена/создана")

        self._execute("""
            CREATE TABLE IF NOT EXISTS hc_vacancies (
                vacancy_id SERIAL PRIMARY KEY,
                hc_vac_id VARCHAR UNIQUE NOT NULL,
                vac_name VARCHAR(255) NOT NULL,
                vac_url TEXT NOT NULL,
                hc_employer_id VARCHAR,
                vac_area TEXT,
                salary_from INTEGER,
                salary_to INTEGER,
                average_salary NUMERIC GENERATED ALWAYS AS (
                    COALESCE((salary_from + salary_to) / 2, salary_from, salary_to)
                ) STORED,
                CONSTRAINT fk_hc_vacancies_hc_employer_id
                FOREIGN KEY(hc_employer_id)
                REFERENCES hc_companies(hc_employer_id)
                ON DELETE CASCADE
                );
            """)
        self._execute("""
            CREATE INDEX IF NOT EXISTS idx_vacancies_employer_id
            ON hc_vacancies (hc_employer_id);
            """)
        self.conn.commit()
        self.logger.info("Tаблица hc_vacancies проверена/создана")

    def _truncate_tables(self) -> None:
        """Полностью очищает таблицы перед загрузкой свежих данных"""
        self._execute("TRUNCATE TABLE hc_vacancies RESTART IDENTITY CASCADE;")
        self._execute("TRUNCATE TABLE hc_companies RESTART IDENTITY CASCADE;")

        self.conn.commit()
        self.logger.info("Таблицы hc_vacancies и hc_companies очищены")

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
