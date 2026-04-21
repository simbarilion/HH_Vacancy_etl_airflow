from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator  # type: ignore

from src.api.hc_json_source import HabrCareerJsonVacanciesSource
from src.database.hc_db_service import HabrCareerDBCreator
from src.notifications.telegram_notifier import TelegramNotifier


def extract_and_transform(**context):
    """Задача 1: Асинхронный сбор и обработка данных"""
    import asyncio

    params = context.get('params', {})  # берём параметры из DAG
    key_word = params.get("key_word", "python")
    max_pages = int(params.get("max_pages", 25))

    async def _extract():
        source = HabrCareerJsonVacanciesSource()
        vacancies, companies = await source.get_formatted_data_async(max_pages=max_pages, key_word=key_word)

        # Передаём данные через XCom
        ti = context["ti"]
        ti.xcom_push(key="vacancies", value=vacancies)
        ti.xcom_push(key="companies", value=companies)
        ti.xcom_push(key="vacancies_count", value=len(vacancies))
        ti.xcom_push(key="companies_count", value=len(companies))
        ti.xcom_push(key="key_word_used", value=key_word)

        return {"vacancies_count": len(vacancies), "companies_count": len(companies)}

    asyncio.run(_extract())


def load_to_postgres(**context):
    """Задача 2: Загрузка данных в PostgreSQL"""
    ti = context["ti"]

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
    ti = context["ti"]
    vac_count = ti.xcom_pull(task_ids="extract_and_transform", key="vacancies_count")
    comp_count = ti.xcom_pull(task_ids="extract_and_transform", key="companies_count")
    key_word = ti.xcom_pull(task_ids="extract_and_transform", key="key_word_used")

    msg = f"""
    ✅ <b>Habr Career ETL Успешно завершён</b>

    Ключ: <code>{key_word}</code>
    Вакансий: <b>{vac_count}</b>
    Работодателей: <b>{comp_count}</b>
        """.strip()

    TelegramNotifier().send_message_sync(msg)
    print(msg)


def notify_failure(context):
    """Отправка уведомления при ошибке (Telegram)"""
    task_instance = context["task_instance"]
    exception = context.get("exception")

    error_msg = f"""
    ❌ <b>Ошибка в ETL Habr Career</b>

    <b>DAG:</b> {task_instance.dag_id}
    <b>Задача:</b> {task_instance.task_id}
    <b>Ошибка:</b> {exception}
        """.strip()

    TelegramNotifier().send_message_sync(error_msg)
    print(error_msg)


# DAG
with DAG(
    dag_id="habr_career_etl",
    start_date=datetime(2026, 4, 1),
    schedule="1 3 * * *",  # каждый день в 13:00 дня
    catchup=False,
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
        "on_failure_callback": notify_failure,
    },
    params={"key_word": "python", "max_pages": 30},  # можно менять в Airflow UI
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
