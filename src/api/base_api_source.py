import threading
from abc import ABC, abstractmethod
from json import JSONDecodeError
from typing import Optional

from curl_cffi import requests as curl_requests
from curl_cffi.requests.exceptions import HTTPError, Timeout, RequestException

from src.logging_config import LoggingConfigClassMixin


class BaseAPISource(ABC, LoggingConfigClassMixin):
    """Базовый класс для работы с API с использованием curl_cffi (обход защиты)"""
    IMPERSONATE = "chrome131"
    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Referer": "https://trudvsem.ru/",
    }

    def __init__(self) -> None:
        super().__init__()
        self.logger = self.configure()
        self.local = threading.local()

    def _create_session(self) -> curl_requests.Session:
        """Создаёт и настраивает session (вызывается 1 раз на поток)"""
        return curl_requests.Session(impersonate=self.IMPERSONATE)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._close_session()

    def _get_session(self) -> curl_requests.Session:
        """Возвращает session для текущего потока"""
        if not hasattr(self.local, "session"):
            self.local.session = self._create_session()
        return self.local.session

    def _close_session(self):
        """Закрывает session текущего потока"""
        if hasattr(self.local, "session"):
            try:
                self.local.session.close()
            except Exception:
                pass
            del self.local.session  # удалить ссылку

    def _get_response(self, url: str, params: dict, headers: Optional[dict] = None) -> Optional[dict]:
        """Выполняет GET запрос через curl_cffi"""
        session = self._get_session()
        try:
            request_headers = {**self.HEADERS, **(headers or {})}
            response = session.get(url, headers=request_headers, params=params, timeout=25, impersonate=self.IMPERSONATE)  # timeout на connect, read
            if response.status_code != 200:
                self.logger.error(
                    f"HTTP {response.status_code} | URL: {response.url} | "
                    f"Response: {response.text[:800]}..."
                )
                if response.status_code in (502, 503, 504):
                    self.logger.info(f"Временная ошибка сервера {response.status_code}. Можно повторить позже.")
                elif response.status_code >= 500:
                    self.logger.error("Серверная ошибка (5xx)")
            response.raise_for_status()
            result = response.json()
            self.logger.debug(f"Успешно получен JSON (страница {params.get('page')})")
            return result

        except HTTPError as err:
            status = getattr(err.response, 'status_code', None)
            if status in (502, 503, 504):
                self.logger.warning(f"Bad Gateway / Service Unavailable (статус {status}) — API временно недоступен")
            else:
                self.logger.error(f"HTTPError: {err} | Status: {status}")
        except Timeout:
            self.logger.error("Timeout при запросе к API")
        except RequestException as err:
            self.logger.error(f"RequestException: {err}")
        except JSONDecodeError as err:
            self.logger.error(f"JSON decode error: {err}")
        except Exception as err:
            self.logger.error(f"Неизвестная ошибка при запросе: {err}")
        return None

    @abstractmethod
    def get_formatted_data(self) -> list[dict]:
        """Возвращает список моделей данных"""
        pass
