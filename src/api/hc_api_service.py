import asyncio

from src.api.hc_api_source import HabrCareerHTMLVacanciesSource
from src.models.vacancy import Vacancy


class HabrCareerHTMLAPI:
    """Получение вакансий и работодателей с HabrCareer"""

    def __init__(self, key_word: str = "", max_pages: int = 5):
        self.max_pages = max_pages
        self.key_word = key_word.strip() if key_word else "python"

    def get_vacs_and_comps(self) -> tuple[list[Vacancy], dict]:
        """Возвращает отформатированные данные о вакансиях и работодателях"""
        return asyncio.run(self._run())

    async def _run(self):
        with HabrCareerHTMLVacanciesSource() as source:
            return await source.get_formatted_data_async(
                self.max_pages,
                self.key_word
            )
