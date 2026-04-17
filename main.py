from config import get_db_name
from src.services.hh_coordinator import HeadHunterDataCoordinator


def main() -> None:
    """Основная функция запуска программы"""
    db_name = get_db_name()
    key_word = "".join(input("Фильтр: ключевое слово / фраза: ").lower())
    max_pages = int(input("Фильтр: количество страниц: "))
    coordinator = HeadHunterDataCoordinator(db_name, key_word, max_pages)
    coordinator.setup_database()

if __name__ == "__main__":
    main()
