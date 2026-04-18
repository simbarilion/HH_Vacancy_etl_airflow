import os

from dotenv import load_dotenv

ROOT_DIR = os.path.dirname(__file__)

LOGS_DIR = os.path.join(ROOT_DIR, "logs")

load_dotenv(os.path.join(ROOT_DIR, ".env"))


def get_db_params() -> dict:
    """Возвращает все параметры для подключения к БД"""
    return {
        "host": os.getenv("DB_HOST", "localhost"),
        "port": os.getenv("DB_PORT", "5432"),
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
    }

def get_db_name() -> str:
    """Возвращает только имя БД"""
    return os.getenv("DB_NAME", "hc_vacancies_employers")

def get_db_base_name() -> str:
    """Имя системной базы, от которой создаём/удаляем БД"""
    return "postgres"
