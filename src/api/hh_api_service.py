from src.api.hh_api_source import HeadHunterVacanciesSource
from src.models.vacancy import Vacancy


class HeadHunterAPI:
    """Получение вакансий и работодателей с HH.ru через API"""

    def __init__(self, key_word: str = "", max_pages: int = 5):
        self.max_pages = max_pages
        self.key_word = self.validate_key_word(key_word)

    def get_vacs_and_comps(self) -> tuple[list[Vacancy], dict]:
        with HeadHunterVacanciesSource() as hh_vac:
            return hh_vac.get_formatted_data(self.max_pages, self.key_word)

    @staticmethod
    def validate_key_word(key_word: str) -> str | None:
        if not key_word or len(key_word) < 2 or len(key_word) > 3000:
            return None
        return str(key_word)
