import asyncio
from src.api.hc_json_source import HabrCareerJsonVacanciesSource
from src.database.hc_db_service import HabrCareerDBCreator

async def run_etl(key_word: str = "python", max_pages: int = 25):
    print(f"Запуск ETL: ключевое слово = '{key_word}', страниц = {max_pages}")
    source = HabrCareerJsonVacanciesSource()
    vacancies, companies = await source.get_formatted_data_async(
        max_pages=max_pages,
        key_word=key_word
    )

    db_creator = HabrCareerDBCreator()
    db_creator.create_and_fill_db(vacancies, companies)

    print(f"Готово! Загружено {len(vacancies)} вакансий и {len(companies)} компаний.")

if __name__ == "__main__":  # python run_etl.py
    asyncio.run(run_etl(key_word="python", max_pages=30))