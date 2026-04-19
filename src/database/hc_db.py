import psycopg2
from psycopg2 import sql

from config import get_db_base_name
from src.database.db_base_connector import DataBaseConnector
from src.logging_config import LoggingConfigClassMixin


class HabrCareerDataBase(DataBaseConnector, LoggingConfigClassMixin):
    """Класс для создания базы данных HabrCareer (вакансии и работодатели)"""

    def __init__(self) -> None:
        super().__init__()
        self._base_dbname: str = get_db_base_name()

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
            conn.close()

    def prepare_tables(self) -> None:
        """Создаёт таблицы (если их нет) и очищает их от старых данных. Всё выполняется в одной транзакции"""
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
        self.logger.info("Tаблица hc_vacancies проверена/создана")

    def _truncate_tables(self) -> None:
        """Полностью очищает таблицы перед загрузкой свежих данных"""
        self._execute("TRUNCATE TABLE hc_vacancies RESTART IDENTITY CASCADE;")
        self._execute("TRUNCATE TABLE hc_companies RESTART IDENTITY CASCADE;")
        self.logger.info("Таблицы hc_vacancies и hc_companies очищены")
