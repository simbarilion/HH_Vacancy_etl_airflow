import time
from concurrent.futures import ThreadPoolExecutor
from functools import partial
import random

from src.api.base_api_source import BaseAPISource
from src.models.employer import Employer
from src.models.vacancy import Vacancy


class TrudVsemVacanciesSource(BaseAPISource):
    """Получение вакансий и работодателей с сайта trudvsem.ru"""

    URL = "http://opendata.trudvsem.ru/api/v1/vacancies"
    BASE_PARAMS = {"limit": 100,}
    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Referer": "https://trudvsem.ru/",
    }

    def __init__(self) -> None:
        super().__init__()
        self.IMPERSONATE = "chrome131"

    def get_formatted_data(self, max_pages: int = 5, key_word: str | None = None) -> tuple[list[Vacancy], dict]:
        """Получает все вакансии и работодателей"""
        all_vacancies: list[Vacancy] = []
        all_employers: dict = {}
        worker = partial(self._get_page, key_word=key_word)

        with ThreadPoolExecutor(max_workers=1) as executor:  # запускает до 5 worker-потоков
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
        """Получает одну страницу через offset с вакансиями и работодателями"""
        vacancies: list[Vacancy] = []
        employers: dict = {}
        offset = page * self.BASE_PARAMS["limit"]
        params = {**self.BASE_PARAMS, "offset": offset}
        if key_word and key_word.strip():
            params["text"] = key_word.strip()
        time.sleep(0.8 + page * 0.4)
        for attempt in range(3):  # до 3 попыток
            data = self._get_response(url=self.URL, headers=self.HEADERS, params=params)
            if data and isinstance(data, dict):
                break  # успех
            if attempt < 2:
                sleep_time = (2 ** attempt) + random.uniform(0.5, 1.5)  # backoff
                self.logger.info(f"Повторная попытка через {sleep_time:.1f} сек (попытка {attempt + 1}/3)")
                time.sleep(sleep_time)
        else:
            self.logger.warning(f"Все попытки не удались (offset={offset})")
            return [], {}
        if not data or not isinstance(data, dict):
            self.logger.warning(f"Не удалось получить данные (offset={offset})")
            return [], {}
        results = data.get("results", [])
        if not results:
            self.logger.info(f"Страница {page} (offset={offset}) пуста")
            return [], {}

        for item in results:
            company = item.get("company") or item.get("hiringOrganization") or {}
            employer_id = str(company.get("id") or company.get("identifier") or company.get("code") or "").strip()
            employer_name = str(company.get("name", "")).strip()
            if not employer_id or not employer_name:
                continue

            vac_id = str(item.get("id") or item.get("identifier", "")).strip()
            name = item.get("name") or item.get("position", "") or item.get("title", "").strip()
            url = str(item.get("url") or item.get("alternateUrl", "")).strip()
            if not vac_id or not name:
                continue

            salary = item.get("salary") or {}
            if isinstance(salary, dict):
                salary_from = salary.get("from") or salary.get("min") or 0
                salary_to = salary.get("to") or salary.get("max") or 0
            else:
                salary_from = salary_to = 0
            try:
                salary_from = int(salary_from) if salary_from not in (None, "", 0) else 0
                salary_to = int(salary_to) if salary_to not in (None, "", 0) else 0
            except (ValueError, TypeError):
                salary_from = salary_to = 0

            address = item.get("address") or {}
            area = ""
            if isinstance(address, dict):
                area = (address.get("city") or
                        address.get("region") or
                        address.get("regionName") or
                        item.get("regionName", ""))
            else:
                area = item.get("regionName", "")
            area = str(area).strip()

            if employer_id not in employers:
                employers[employer_id] = Employer(
                    employer_id=employer_id,
                    name=employer_name or "Не указан",
                    url=company.get("url", "") or f"https://trudvsem.ru/organization/{employer_id}"
                )

            vacancies.append(
                Vacancy(
                    vac_id=vac_id,
                    name=name.strip() if name else "Без названия",
                    url=url,
                    salary_from=int(salary_from) if salary_from else 0,
                    salary_to=int(salary_to) if salary_to else 0,
                    area=str(area).strip(),
                    employer_id=employer_id,
                )
            )
        return vacancies, employers
