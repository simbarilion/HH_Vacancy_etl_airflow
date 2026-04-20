from src.api.hc_html_source import HabrCareerHTMLVacanciesSource
from src.database.hc_db_service import HabrCareerDBCreator


async def main():
    key_word = input("Фильтр: ключевое слово / фраза: ").strip()
    max_pages = int(input("Фильтр: количество страниц: "))

    source = HabrCareerHTMLVacanciesSource()
    vacancies, companies = await source.get_formatted_data_async(max_pages=max_pages, key_word=key_word)

    db = HabrCareerDBCreator()
    db.create_and_fill_db(vacancies, companies)


if __name__ == "__main__":
    main()
