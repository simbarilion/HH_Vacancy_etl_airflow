from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

from src.api.hc_api_source import HabrCareerHTMLVacanciesSource
from src.database.hc_db_service import HabrCareerDBCreator


def extract_and_transform(**context):
    """Задача 1: Асинхронный сбор и обработка данных"""
    import asyncio

    async def _extract():
        source = HabrCareerHTMLVacanciesSource()
        vacancies, companies = await source.get_formatted_data_async(
            max_pages=30,
            key_word="python"
        )

        # Передаём данные через XCom
        ti = context['ti']
        ti.xcom_push(key="vacancies", value=vacancies)
        ti.xcom_push(key="companies", value=companies)
        ti.xcom_push(key="vacancies_count", value=len(vacancies))
        ti.xcom_push(key="companies_count", value=len(companies))

        return {"vacancies_count": len(vacancies), "companies_count": len(companies)}

    asyncio.run(_extract())


def load_to_postgres(**context):
    """Задача 2: Загрузка данных в PostgreSQL"""
    ti = context['ti']

    # Получаем данные из XCom
    vacancies = ti.xcom_pull(task_ids="extract_and_transform", key="vacancies")
    companies = ti.xcom_pull(task_ids="extract_and_transform", key="companies")

    if not vacancies or not companies:
        raise ValueError("Нет данных для загрузки в базу!")

    db_creator = HabrCareerDBCreator()
    db_creator.create_and_fill_db(vacancies, companies)

    print(f"Успешно загружено в БД: {len(vacancies)} вакансий и {len(companies)} компаний")


def notify_success(**context):
    """Задача 3: Уведомление об успешном завершении"""
    ti = context['ti']
    vac_count = ti.xcom_pull(task_ids="extract_and_transform", key="vacancies_count")
    comp_count = ti.xcom_pull(task_ids="extract_and_transform", key="companies_count")

    print(f"   ETL Habr Career успешно завершён!")
    print(f"   Вакансий загружено: {vac_count}")
    print(f"   Работодателей: {comp_count}")


def notify_failure(context):
    """Отправка уведомления при ошибке (можно в Telegram/Slack/почту)"""
    print("Ошибка в ETL!")
    print(f"Задача: {context['task_instance'].task_id}")
    print(f"Ошибка: {context.get('exception')}")


# DAG
with DAG(
        dag_id="habr_career_etl",
        start_date=datetime(2026, 4, 1),
        schedule_interval="0 3 * * *",  # каждый день в 3:00 ночи
        catchup=False,
        default_args={
            "retries": 3,
            "retry_delay": timedelta(minutes=5),
            "on_failure_callback": notify_failure,
            "owner": "nadezhda",
        },
        tags=["habr", "vacancies", "etl"],
        max_active_runs=1,  # не запускать несколько одновременно
) as dag:
    extract_task = PythonOperator(
        task_id="extract_and_transform",
        python_callable=extract_and_transform,
        provide_context=True,
    )

    load_task = PythonOperator(
        task_id="load_to_postgres",
        python_callable=load_to_postgres,
        provide_context=True,
    )

    notify_task = PythonOperator(
        task_id="notify_success",
        python_callable=notify_success,
        provide_context=True,
    )

    # Порядок выполнения
    extract_task >> load_task >> notify_task
