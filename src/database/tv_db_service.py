from src.database.tv_db import TrudVsemDataBase
from src.models.vacancy import Vacancy


class TrudVsemDBCreator:
    """Создает и заполняет БД"""

    def __init__(self, db_name: str):
        self._db_name = db_name
        self._db = TrudVsemDataBase(db_name)

    @property
    def db_name(self) -> str:
        """Возвращает название базы данных с компаниями и вакансиями сайта trudvsem.ru"""
        return self._db_name

    def create_and_fill_db(self, vacancies: list[Vacancy], companies: dict) -> None:
        self._db.create_database()
        with self._db:
            self._db.prepare_tables()
            self._db.save_data_to_table_tv_companies(companies)
            self._db.save_data_to_table_tv_vacancies(vacancies)
