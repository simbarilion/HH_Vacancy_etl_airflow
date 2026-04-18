from src.database.hc_db import HabrCareerDataBase
from src.models.vacancy import Vacancy


class HabrCareerDBCreator:
    def __init__(self):
        self._db = HabrCareerDataBase()

    def create_and_fill_db(self, vacancies: list[Vacancy], companies: dict) -> None:
        """Создает и заполняет БД"""
        self._db.create_database()
        with self._db:
            self._db.prepare_tables()
            self._db.save_data_to_table_hc_companies(companies)
            self._db.save_data_to_table_hc_vacancies(vacancies)
