from src.api.hc_api_source import HabrCareerHTMLVacanciesSource
from src.models.vacancy import Vacancy


class HabrCareerHTMLAPI:
    """Получение вакансий и работодателей с HabrCareer"""

    def __init__(self, key_word: str = "", max_pages: int = 5):
        self.max_pages = max_pages
        self.key_word = key_word.strip() if key_word else "python"

    def get_vacs_and_comps(self) -> tuple[list[Vacancy], dict]:
        """Возвращает отформатированные данные о вакансиях и работодателях"""
        with HabrCareerHTMLVacanciesSource() as hc_vac:
            return hc_vac.get_formatted_data(self.max_pages, self.key_word)
