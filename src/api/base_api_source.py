import threading
from abc import ABC, abstractmethod
from json import JSONDecodeError
from typing import Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3 import Retry

from src.logging_config import LoggingConfigClassMixin


class BaseAPISource(ABC, LoggingConfigClassMixin):
    """Базовый класс для работы с API"""
    def __init__(self) -> None:
        super().__init__()
        self.logger = self.configure()
        self.local = threading.local()

    def _create_session(self) -> requests.Session:
        """Создаёт и настраивает session (вызывается 1 раз на поток)"""
        session = requests.Session()

        retry = Retry(
            total=3,  # максимум 3 попытки
            backoff_factor=1,  # задержка между повторами
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
        )
        adapter = HTTPAdapter(  # определяет политику повторных попыток
            max_retries=retry,
            pool_connections=10,
            pool_maxsize=10
        )

        session.mount("https://", adapter)
        session.mount("http://", adapter)

        return session

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def _get_session(self) -> requests.Session:
        """Возвращает session для текущего потока"""
        if not hasattr(self.local, "session"):
            self.local.session = self._create_session()
        return self.local.session

    def _close_session(self):
        """Закрывает session текущего потока"""
        if hasattr(self.local, "session"):
            self.local.session.close()
            del self.local.session  # удалить ссылку

    def _get_response(self, url: str, params: dict, headers: Optional[dict] = None) -> Optional[dict]:
        """Выполняет GET запрос и возвращает JSON"""
        session = self._get_session()
        try:
            response = session.get(url, headers=headers, params=params, timeout=(3, 10))  # timeout на connect, read
            response.raise_for_status()
            result = response.json()
            self.logger.debug("JSON получен")
            return result
        except requests.exceptions.Timeout:
            self.logger.error("Timeout error")
        except requests.exceptions.RequestException as err:
            self.logger.error(f"HTTP error: {err}")
        except JSONDecodeError as err:
            self.logger.error(f"JSON decode error: {err}")
        return None

    @abstractmethod
    def get_formatted_data(self) -> list[dict]:
        """Возвращает список моделей данных"""
        pass
