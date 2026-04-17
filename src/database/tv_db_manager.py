from typing import Optional

import psycopg2

from config import config
from src.logging_config import LoggingConfigClassMixin


class TrudVsemDataBaseManager(LoggingConfigClassMixin):
    """Класс для создания запросов к базе данных с компаниями и вакансиями trudvsem.ru"""

    def __init__(self, db_name: str) -> None:
        super().__init__()
        self._tv_dbname = db_name
        self._params: dict = self._get_params()
        self._conn = None
        self.logger = self.configure()

    def _ensure_connection(self) -> None:
        """Открывает соединение с базой данных при первом запросе"""
        if self._conn and not self._conn.closed:
            return
        try:
            self._conn = psycopg2.connect(**self._params, dbname=self._tv_dbname)
            self.logger.info("Соединение с базой данных открыто")
        except psycopg2.Error as e:
            self.logger.error(f"Ошибка подключения к базе данных: {e}")
            raise

    def close_connection(self) -> None:
        """Закрывает соединение с базой данных"""
        if self._conn:
            self._conn.close()
            self.logger.info("Соединение с базой данных закрыто")

    def _execute_query(self, query: str | tuple, params: Optional[tuple] = None) -> Optional[list[tuple]]:
        """Выполняет запрос к базе данных и возвращает результат"""
        self._ensure_connection()
        try:
            with self._conn.cursor() as cur:
                cur.execute(query, params)
                if cur.description:  # есть ли результаты
                    rows = cur.fetchall()
                else:
                    rows = []
            self._conn.commit()
            self.logger.info(f"Запрос к базе данных {self._tv_dbname} выполнен успешно")
            return rows  # type: ignore
        except psycopg2.Error as e:
            self._conn.rollback()
            self.logger.error(f"Ошибка при выполнении запроса: {e}")
            raise

    def get_companies_and_vacancies_count(self) -> Optional[list]:
        """Получает список всех компаний и количество вакансий у каждой компании"""
        query = """
                SELECT c.employer_name, COUNT(v.vacancy_id) AS vacancies_count, c.employer_url
                FROM tv_companies AS c
                LEFT JOIN tv_vacancies AS v ON c.tv_employer_id = v.tv_employer_id
                GROUP BY c.employer_name, c.employer_url
                ORDER BY c.employer_name;
                """
        return self._execute_query(query)

    def get_all_vacancies(self) -> list:
        """Получает список всех вакансий"""
        query = """
                SELECT c.employer_name, v.vac_name, v.salary_from, v.salary_to, v.vac_area, v.vac_url
                FROM tv_companies as c
                JOIN tv_vacancies as v ON c.tv_employer_id = v.tv_employer_id
                ORDER BY c.employer_name, v.vac_name;
                """
        return self._execute_query(query)

    def get_avg_salary(self) -> list:
        """Получает среднюю зарплату по вакансиям у каждой компании"""
        query = """
                SELECT c.employer_name, AVG(v.average_salary) AS average_salary, c.employer_url
                FROM tv_companies AS c
                LEFT JOIN tv_vacancies AS v ON c.tv_employer_id = v.tv_employer_id
                GROUP BY c.employer_name, c.employer_url
                ORDER BY average_salary DESC;
                """
        return self._execute_query(query)

    def get_vacancies_with_higher_salary(self) -> list:
        """Получает список всех вакансий, у которых зарплата выше средней по всем вакансиям"""
        query = """
                SELECT c.employer_name, v.vac_name, v.salary_from, v.salary_to, v.vac_area, v.vac_url
                FROM tv_vacancies AS v
                JOIN tv_companies AS c ON
                c.tv_employer_id = v.tv_employer_id
                JOIN (SELECT AVG(average_salary) AS avg_salary FROM tv_vacancies) AS s
                ON v.average_salary > s.avg_salary
                ORDER BY v.average_salary DESC;
                """
        return self._execute_query(query)

    def get_vacancies_with_keyword(self, key_word: str) -> list:
        """Получает список всех вакансий по ключевому слову в названии"""
        query = """
                SELECT c.employer_name, v.vac_name, v.salary_from, v.salary_to, v.vac_area, v.vac_url
                FROM tv_vacancies AS v
                JOIN tv_companies AS c ON c.tv_employer_id = v.tv_employer_id
                WHERE v.vac_name ILIKE %s
                ORDER BY c.employer_name, v.vac_name"""
        params = (f"%{key_word}%",)
        return self._execute_query(query, params)

    @staticmethod
    def _get_params() -> dict:
        """Возвращает параметры подключения к базе данных"""
        return config()
