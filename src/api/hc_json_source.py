import asyncio
import random

import httpx

from src.logging_config import LoggingConfigClassMixin
from src.models.employer import Employer
from src.models.vacancy import Vacancy


class HabrCareerJsonVacanciesSource(LoggingConfigClassMixin):
    """Парсинг вакансий с Habr Career через JSON API"""

    BASE_URL = "https://career.habr.com/api/frontend/vacancies"
    MAX_CONCURRENT = 6
    MIN_DELAY = 0.7
    MAX_DELAY = 2.2

    def __init__(self) -> None:
        super().__init__()
        self.logger = self.configure()
        self.semaphore = asyncio.Semaphore(self.MAX_CONCURRENT)

    async def get_formatted_data_async(self, max_pages: int = 30, key_word: str | None = None) -> tuple[list[Vacancy], dict]:
        """Основной публичный метод"""
        all_vacancies: list[Vacancy] = []
        all_employers: dict = {}
        batch_size = 5
        empty_streak = 0

        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                          "(KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8",
            "Referer": "https://career.habr.com/vacancies?q=python",
            "Origin": "https://career.habr.com",
        }

        async with httpx.AsyncClient(headers=headers, timeout=20.0, follow_redirects=True) as client:
            for start in range(0, max_pages, batch_size):
                batch = list(range(start, min(start + batch_size, max_pages)))
                tasks = [self._get_page_async(client, page, key_word) for page in batch]
                results = await asyncio.gather(*tasks, return_exceptions=True)

                batch_has_data = False

                for result in results:
                    if isinstance(result, Exception):
                        self.logger.error(f"Задача упала: {result}")
                        continue

                    vacancies, employers = result
                    if vacancies:
                        batch_has_data = True

                    all_vacancies.extend(vacancies)
                    for emp_id, emp in employers.items():
                        if emp_id and emp_id not in all_employers:
                            all_employers[emp_id] = emp
                if not batch_has_data:
                    empty_streak += 1
                else:
                    empty_streak = 0
                if empty_streak >= 2:
                    self.logger.info(f"Остановка на батче {batch} — больше вакансий нет")
                    break

        self.logger.info(f"HabrCareer JSON. Всего вакансий: {len(all_vacancies)} | Работодателей: {len(all_employers)}")
        return all_vacancies, all_employers

    async def _get_page_async(self, client: httpx.AsyncClient, page: int, key_word: str | None) -> tuple[list[Vacancy], dict]:
        """Один запрос с retry"""
        for attempt in range(3):
            async with self.semaphore:
                try:
                    await asyncio.sleep(random.uniform(self.MIN_DELAY, self.MAX_DELAY + attempt * 0.5))
                    params = {
                        "q": key_word or "python",
                        "page": page + 1,
                        "sort": "date",
                        "per_page": 25,
                        "type": "all"
                    }
                    resp = await client.get(self.BASE_URL, params=params)
                    self.logger.info(f"Страница {page + 1}: статус {resp.status_code}, размер ответа {len(resp.text)}")
                    if resp.status_code != 200:
                        self.logger.warning(f"HTTP {resp.status_code}: {resp.text[:400]}")
                        await asyncio.sleep(1.5)
                        continue
                    data = resp.json()
                    return self._parse_json_page(data, page)

                except Exception as e:
                    self.logger.warning(f"Попытка {attempt+1}/3 страницы {page+1} не удалась: {e}")
                    await asyncio.sleep(2 ** attempt)

        self.logger.error(f"Не удалось загрузить страницу {page+1} после 3 попыток")
        return [], {}

    def _parse_json_page(self, data: dict, page: int) -> tuple[list[Vacancy], dict]:
        """Парсинг JSON-ответа"""
        vacancies: list[Vacancy] = []
        employers: dict = {}

        items = data.get("list", [])
        if not items:
            self.logger.warning(f"Страница {page + 1}: ключ 'list' пустой или отсутствует")
            self.logger.debug(f"Ключи верхнего уровня: {list(data.keys())}")
            return [], {}
        self.logger.info(f"Страница {page + 1}: получено {len(items)} вакансий из JSON")

        for item in items:
            try:
                vac_id = str(item.get("id"))
                vac_name = item.get("title", "").strip()
                vac_url = "https://career.habr.com" + item.get("href", "")

                company = item.get("company") or {}
                employer_id = str(company.get("id") or f"habr_{company.get('alias_name', 'unknown')}")
                employer_name = company.get("title", "Не указана")
                employer_url = "https://career.habr.com" + company.get("href", "") if company.get("href") else ""

                # Зарплата
                salary = item.get("salary") or {}
                salary_from = salary.get("from") or 0
                salary_to = salary.get("to") or salary_from
                if salary_from > 0 and salary_to == 0:
                    salary_to = salary_from

                # Регион
                locations = item.get("locations") or []
                area = locations[0].get("title") if locations else ""
                if not area:
                    qual = item.get("salaryQualification") or {}
                    area = qual.get("title", "")

                if employer_name == "Не указана" or not vac_id:
                    continue

                if employer_id not in employers:
                    employers[employer_id] = Employer(
                        employer_id=employer_id,
                        name=employer_name,
                        url=employer_url
                    )

                vacancies.append(Vacancy(
                    vac_id=vac_id,
                    name=vac_name,
                    url=vac_url,
                    salary_from=int(salary_from) if salary_from else 0,
                    salary_to=int(salary_to) if salary_to else 0,
                    area=str(area).strip(),
                    employer_id=employer_id,
                ))

            except Exception as e:
                self.logger.debug(f"Ошибка парсинга вакансии #{page+1}: {e}")
                continue

        return vacancies, employers
