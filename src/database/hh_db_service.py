from src.database.hh_db import HeadHunterDataBase
from src.models.vacancy import Vacancy


class HeadHunterDBCreator:
    """Создает и заполняет БД"""

    def __init__(self, db_name: str):
        self._db_name = db_name
        self._db = HeadHunterDataBase(db_name)

    @property
    def db_name(self) -> str:
        """Возвращает название базы данных с компаниями и вакансиями сайта HeadHunter.ru"""
        return self._db_name

    def create_and_fill_db(self, vacancies: list[Vacancy], companies: dict) -> None:
        self._db.create_database()
        with self._db:
            self._db.create_table_hh_companies()
            self._db.create_table_hh_vacancies()
            self._db.save_data_to_table_hh_companies(companies)
            self._db.save_data_to_table_hh_vacancies(vacancies)
