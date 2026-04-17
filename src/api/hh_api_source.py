import time
from concurrent.futures import ThreadPoolExecutor
from functools import partial

from src.api.base_api_source import BaseAPISource
from src.models.employer import Employer
from src.models.vacancy import Vacancy


class HeadHunterVacanciesSource(BaseAPISource):
    """Получение вакансий работодателей HH"""

    URL = "https://api.hh.ru/vacancies"
    HEADERS = {"User-Agent": "api-test-agent"}
    BASE_PARAMS = {"per_page": 100, "only_with_salary": True, "currency": "RUR", "area": 113}

    def __init__(self) -> None:
        super().__init__()

    def get_formatted_data(self, max_pages: int = 5, key_word: str | None = None) -> tuple[list[Vacancy], dict]:
        """Получает все вакансии и работодателей"""
        all_vacancies: list[Vacancy] = []
        all_employers: dict = {}
        worker = partial(self._get_page, key_word=key_word)

        with ThreadPoolExecutor(max_workers=2) as executor:  # запускает до 5 worker-потоков
            results = executor.map(worker, range(max_pages))  # запросы идут параллельно
            for vacancies, employers in results:
                all_vacancies.extend(vacancies)

                for emp_id, emp in employers.items():
                    if emp_id not in all_employers:
                        all_employers[emp_id] = emp

        self.logger.info(f"Всего вакансий: {len(all_vacancies)}")
        self.logger.info(f"Всего работодателей: {len(all_employers)}")
        return all_vacancies, all_employers

    def _get_page(self, page: int, key_word: str | None = None) -> tuple[list[Vacancy], dict]:
        """Проходит по странице API и собирает все вакансии и работодателей"""
        vacancies: list[Vacancy] = []
        employers: dict = {}
        params = {**self.BASE_PARAMS, "page": page}
        if key_word:
            params["text"] = key_word
        time.sleep(0.3 + page * 0.05)
        data = self._get_response(url=self.URL, headers=self.HEADERS, params=params)
        if not data:
            self.logger.warning(f"Не удалось получить данные с API (страница {page})")
            return [], {}
        if page >= data.get("pages", 0):
            return [], {}
        for vac in data.get("items", []):
            salary = vac.get("salary")
            if not salary or salary.get("currency") != "RUR":
                continue

            emp = vac["employer"]
            if emp["id"] not in employers:
                employers[emp["id"]] = Employer(
                    employer_id=str(emp.get("id")),
                    name=emp.get("name", ""),
                    url=emp.get("alternate_url", "")
                )
            vacancies.append(
                Vacancy(
                    vac_id=str(vac.get("id")),
                    name=vac.get("name", ""),
                    url=vac.get("alternate_url", ""),
                    salary_from=salary.get("from") or 0,
                    salary_to=salary.get("to") or 0,
                    area=vac.get("area", {}).get("name", ""),
                    employer_id=emp["id"],
                )
            )
        return vacancies, employers
