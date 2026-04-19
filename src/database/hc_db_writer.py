from psycopg2.extras import execute_batch

from src.database.db_base_connector import DataBaseConnector
from src.logging_config import LoggingConfigClassMixin
from src.models.vacancy import Vacancy


class HabrCareerDBWriter(LoggingConfigClassMixin):
    """Класс для заполнения базы данных HabrCareer вакансиями и работодателями"""

    def __init__(self, connector: DataBaseConnector) -> None:
        super().__init__()
        self.connector = connector
        self.logger = self.configure()

    def _batch_insert(self, query: str, data: list[tuple]) -> None:
        """Универсальная массовая вставка данных"""
        if not data:
            return
        with self.connector.conn.cursor() as cur:
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
