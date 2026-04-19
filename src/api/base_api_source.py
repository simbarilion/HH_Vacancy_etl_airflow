from abc import ABC, abstractmethod
from json import JSONDecodeError
from typing import Optional

from curl_cffi import requests as curl_requests
from curl_cffi.requests.exceptions import HTTPError, Timeout, RequestException

from src.logging_config import LoggingConfigClassMixin


class BaseAPISource(ABC, LoggingConfigClassMixin):
    """Базовый класс для парсинга сайтов (HTML) с использованием curl_cffi"""
    IMPERSONATE = "chrome131"
    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Referer": "https://career.habr.com/",
        "Cache-Control": "no-cache",
    }

    def __init__(self) -> None:
        super().__init__()
        self.logger = self.configure()
        self.session = None

    def _create_session(self) -> curl_requests.Session:
        """Создаёт и настраивает session (вызывается 1 раз на поток)"""
        return curl_requests.Session(impersonate=self.IMPERSONATE)

    def __enter__(self):
        self.session = self._create_session()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            try:
                self.session.close()
            except Exception:
                pass

    def _get_session(self) -> curl_requests.Session:
        """Возвращает session для текущего потока"""
        if not self.session:
            raise RuntimeError("Session not initialized")
        return self.session

    def _get_response(self, url: str, params: dict, headers: Optional[dict] = None) -> Optional[str]:
        """Возвращает HTML-текст страницы"""
        session = self._get_session()
        try:
            request_headers = {**self.HEADERS, **(headers or {})}
            response = session.get(url, headers=request_headers, params=params or {}, timeout=25, impersonate=self.IMPERSONATE)  # timeout на connect, read
            if response.status_code != 200:
                self.logger.warning(
                    f"HTTP {response.status_code} | URL: {response.url} | "
                    f"Response: {response.text[:800]}..."
                )
            response.raise_for_status()
            return response.text

        except HTTPError as err:
            self.logger.error(f"HTTPError: {err} | Status: {getattr(err.response, 'status_code', None)}")
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
    def get_formatted_data_async(self) -> list[dict]:
        """Возвращает список моделей данных"""
        pass
