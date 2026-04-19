import asyncio
import random
import re

from bs4 import BeautifulSoup

from src.api.base_api_source import BaseAPISource
from src.models.employer import Employer
from src.models.vacancy import Vacancy


class HabrCareerHTMLVacanciesSource(BaseAPISource):
    """Парсинг вакансий с career.habr.com"""
    BASE_URL = "https://career.habr.com/vacancies"
    MAX_CONCURRENT = 5
    MIN_DELAY = 0.6
    MAX_DELAY = 1.8

    def __init__(self) -> None:
        super().__init__()
        self.semaphore = asyncio.Semaphore(self.MAX_CONCURRENT)
        self.IMPERSONATE = "chrome131"

    async def get_formatted_data_async(self, max_pages: int = 5, key_word: str | None = None) -> tuple[list[Vacancy], dict]:
        """Получает все вакансии и работодателей"""
        all_vacancies: list[Vacancy] = []
        all_employers: dict = {}
        batch_size = 5
        empty_streak = 0

        for start in range(0, max_pages, batch_size):
            batch = list(range(start, min(start + batch_size, max_pages)))
            tasks = [self._get_page_async(page, key_word) for page in batch]
            results = await asyncio.gather(*tasks)

            batch_has_data = False
            for vacancies, employers in results:
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
            if empty_streak >= 1:
                self.logger.info(f"Остановка на батче {batch}")
                break

        self.logger.info(f"HabrCareer HTML. Всего вакансий: {len(all_vacancies)}")
        self.logger.info(f"HabrCareer HTML. Всего работодателей: {len(all_employers)}")
        return all_vacancies, all_employers

    async def _get_page_async(self, page: int, key_word: str) -> tuple[list[Vacancy], dict]:
        for attempt in range(3):
            async with self.semaphore:
                try:
                    await asyncio.sleep(random.uniform(self.MIN_DELAY, self.MAX_DELAY + attempt))
                    return await asyncio.to_thread(
                        self._get_page_sync,
                        page,
                        key_word
                    )
                except Exception:
                    await asyncio.sleep(2 ** attempt)
        return [], {}

    def _get_page_sync(self, page: int, key_word: str | None = None) -> tuple[list[Vacancy], dict]:
        """Получает одну страницу через offset с вакансиями и работодателями"""
        vacancies: list[Vacancy] = []
        employers: dict = {}
        params = {
            "q": key_word or "python",
            "page": page + 1,  # на сайте пагинация начинается с 1
            "sort": "date"
        }

        html_text = self._get_response(url=self.BASE_URL, params=params)

        if not html_text:
            self.logger.warning(f"Не удалось загрузить страницу {page + 1}")
            return [], {}

        if page == 0:
            with open("habr_page_debug.html", "w", encoding="utf-8") as f:
                f.write(html_text)

        soup = BeautifulSoup(html_text, "lxml")
        cards = soup.select("article.vacancy-card, div.vacancy-card, div.tm-vacancy-card")
        if not cards:
            self.logger.warning(f"Страница {page + 1}: карточки вакансий не найдены")
            return [], {}

        for card in cards:
            try:
                title_link = card.select_one("a.vacancy-card__title-link") # Название и ссылка
                if not title_link:
                    continue
                relative_url = title_link.get("href", "").strip()
                vac_url = f"https://career.habr.com{relative_url}" if relative_url.startswith("/") else relative_url
                vac_name = title_link.get_text(strip=True)
                vac_id = relative_url.strip("/").split("/")[-1] if relative_url else ""
                if not vac_id or not vac_name:
                    continue

                company_link = card.select_one("div.vacancy-card__company a.link-comp")
                employer_name = company_link.get_text(strip=True) if company_link else "Не указана"
                employer_href = company_link.get("href", "") if company_link else ""
                employer_id_match = re.search(r'/companies/(\d+)', employer_href)
                if employer_id_match:
                    employer_id = employer_id_match.group(1)
                else:  # Если числового ID нет — берём slug и добавляем префикс
                    slug = employer_href.strip("/").split("/")[-1] if employer_href else ""
                    employer_id = f"habr_{slug}" if slug else f"habr_unknown_{hash(employer_name) % 100000}"
                employer_url = f"https://career.habr.com{employer_href}" if employer_href.startswith(
                    "/") else employer_href
                if not employer_name or employer_name == "Не указана":
                    continue

                salary_tag = card.select_one("div.vacancy-card__salary")  # Зарплата
                salary_text = salary_tag.get_text(strip=True) if salary_tag else ""
                salary_from, salary_to = self._parse_salary(salary_text)
                if salary_from > 0 and salary_to == 0:
                    salary_to = salary_from

                area = ""
                meta = card.select_one("div.vacancy-card__meta")  # Регион
                if meta:
                    chips = meta.select("div.chip-with-icon__text")
                    for chip in chips:
                        text = chip.get_text(strip=True)
                        if text and text not in ["Senior", "Middle", "Lead", "Intern", "Junior", "Можно удалённо"]:
                            area = text
                            break

                if employer_id and employer_name:
                    if employer_id not in employers:
                        employers[employer_id] = Employer(
                            employer_id=employer_id,
                            name=employer_name,
                            url=employer_url or ""
                        )

                vacancies.append(
                    Vacancy(
                        vac_id=vac_id,
                        name=vac_name,
                        url=vac_url,
                        salary_from=salary_from,
                        salary_to=salary_to,
                        area=area,
                        employer_id=employer_id,
                    )
                )
            except Exception as e:
                self.logger.warning(f"Ошибка парсинга карточки: {e}")
                continue
        return vacancies, employers

    def _parse_salary(self, salary_text: str) -> tuple[int, int]:
        """Парсит зарплату с Habr Career"""
        if not salary_text:
            return 0, 0
        numbers = re.findall(r'\d+', salary_text.replace(' ', ''))
        numbers = [int(n) for n in numbers]

        if not numbers:
            return 0, 0
        if len(numbers) == 1:
            return numbers[0], 0
        return min(numbers), max(numbers)
