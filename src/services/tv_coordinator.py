from src.api.tv_api_service import TrudVsemAPI
from src.database.tv_db_service import TrudVsemDBCreator


class TrudVsemDataCoordinator:
    """
    Связывает API и DB Creator, отвечает за workflow:
    получение данных, создание/заполнение БД
    """

    def __init__(self, db_name: str, key_word: str = "", max_pages: int = 5):
        """Конструктор класса TrudVsemDataCoordinator"""
        self.api = TrudVsemAPI(key_word, max_pages)
        self.db_creator = TrudVsemDBCreator(db_name)

    def setup_database(self) -> None:
        print("Начинаю поиск")
        vacancies, companies = self.api.get_vacs_and_comps()
        print(f"Получено {len(vacancies)}, {len(companies)}")
        self.db_creator.create_and_fill_db(vacancies, companies)
