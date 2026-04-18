from src.api.hc_api_service import HabrCareerHTMLAPI
from src.database.hc_db_service import HabrCareerDBCreator


class HabrCareerDataCoordinator:
    """
    Связывает API и DB Creator, отвечает за workflow:
    получение данных, создание/заполнение БД
    """

    def __init__(self, key_word: str = "", max_pages: int = 5):
        """Конструктор класса HabrCareerDataCoordinator"""
        self.api = HabrCareerHTMLAPI(key_word, max_pages)
        self.db_creator = HabrCareerDBCreator()

    def setup_database(self) -> None:
        print("Начинаю поиск")
        vacancies, companies = self.api.get_vacs_and_comps()
        print(f"Получено {len(vacancies)}, {len(companies)}")
        self.db_creator.create_and_fill_db(vacancies, companies)
