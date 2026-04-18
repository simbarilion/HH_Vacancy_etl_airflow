from src.database.hc_db import HabrCareerDataBase
from src.models.vacancy import Vacancy


class HabrCareerDBCreator:
    """Создает и заполняет БД"""

    def __init__(self, db_name: str):
        self._db_name = db_name
        self._db = HabrCareerDataBase(db_name)

    @property
    def db_name(self) -> str:
        """Возвращает название базы данных с компаниями и вакансиями сайта HabrCareer"""
        return self._db_name

    def create_and_fill_db(self, vacancies: list[Vacancy], companies: dict) -> None:
        self._db.create_database()
        with self._db:
            self._db.prepare_tables()
            self._db.save_data_to_table_hc_companies(companies)
            self._db.save_data_to_table_hc_vacancies(vacancies)
