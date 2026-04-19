from src.database.hc_db import HabrCareerDataBase
from src.database.hc_db_writer import HabrCareerDBWriter
from src.logging_config import LoggingConfigClassMixin
from src.models.vacancy import Vacancy


class HabrCareerDBCreator(LoggingConfigClassMixin):
    def __init__(self):
        super().__init__()
        self._db = HabrCareerDataBase()
        self.logger = self.configure()

    def create_and_fill_db(self, vacancies: list[Vacancy], companies: dict) -> None:
        """
        Создаёт базу, подготавливает таблицы и заполняет данными.
        Подготовка таблиц и заполнение данных — в отдельных транзакциях
        """
        self._db.create_database()

        with self._db as connector:
            try:
                self._db.prepare_tables()
                writer = HabrCareerDBWriter(connector)
                writer.save_data_to_table_hc_companies(companies)
                writer.save_data_to_table_hc_vacancies(vacancies)
                connector.conn.commit()
                self.logger.info(f"Данные успешно сохранены: {len(companies)} компаний, {len(vacancies)} вакансий")

            except Exception as e:
                if connector.conn:
                    connector.conn.rollback()
                self.logger.error(f"Ошибка при заполнении базы данных. Изменения откатаны: {e}")
                raise
