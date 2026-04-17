import os
from configparser import ConfigParser

from dotenv import load_dotenv

ROOT_DIR = os.path.dirname(__file__)

LOGS_DIR = os.path.join(ROOT_DIR, "logs")

load_dotenv(os.path.join(ROOT_DIR, ".env"))


def config(filename: str = "database.ini", section: str = "postgresql") -> dict:
    """Возвращает параметры для подключения к БД"""
    parser = ConfigParser()
    parser.read(os.path.join(ROOT_DIR, filename), encoding="utf-8")

    if not parser.has_section(section):
        raise Exception(f"Section {section} is not found in the {filename} file")

    db_config = {key: value.strip() for key, value in parser.items(section)}

    db_user = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")
    if db_user is None or db_password is None:
        raise EnvironmentError("Переменная окружения не установлена")

    db_config["user"] = db_user
    db_config["password"] = db_password

    return db_config

def get_db_name() -> str:
    return os.getenv("DB_NAME", "tv_vacancies_employers")
