from config import get_db_name
from src.services.hc_coordinator import HabrCareerDataCoordinator


def main() -> None:
    """Основная функция запуска программы"""
    key_word = "".join(input("Фильтр: ключевое слово / фраза: ").lower())
    max_pages = int(input("Фильтр: количество страниц: "))
    coordinator = HabrCareerDataCoordinator(key_word, max_pages)
    coordinator.setup_database()

if __name__ == "__main__":
    main()
