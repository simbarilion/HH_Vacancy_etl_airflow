# Habr Career Vacancy ETL

Автоматический сбор вакансий с сайта **career.habr.com** с последующим сохранением в PostgreSQL и отправкой уведомлений в Telegram.

Проект реализован как полноценный **ETL-процесс** с использованием Apache Airflow.

---

## О проекте

Проект предназначен для регулярного сбора актуальных вакансий с Habr Career по заданным ключевым словам.  
Собранные данные парсятся, очищаются и сохраняются в базу данных. При возникновении ошибок отправляется уведомление в Telegram.

Основная цель — автоматизация мониторинга рынка труда в IT-сфере с удобным хранением и анализом данных.

---

## Реализованный функционал

- Асинхронный сбор вакансий через официальный JSON API Habr Career
- Парсинг названий, зарплат, регионов, компаний и квалификаций
- Корректная обработка зарплат (если указана только "от" — "до" приравнивается к "от")
- Сохранение данных в нормализованную структуру PostgreSQL (компании + вакансии)
- Автоматическая очистка таблиц перед каждой загрузкой
- Уведомления в Telegram об успешном завершении и ошибках
- Запуск по расписанию через Apache Airflow
- Возможность запуска вручную (локально и в Docker)
- Поддержка параметров DAG (ключевые слова и количество страниц)

---

## Технологический стек

- **Python 3.12**
- **Apache Airflow 2.10** — оркестрация ETL
- **httpx** — асинхронные HTTP-запросы
- **BeautifulSoup** (резерв) + прямой JSON-парсинг
- **PostgreSQL** — хранение данных
- **psycopg2** — взаимодействие с базой данных
- **Telegram Bot API** — уведомления
- **Docker + Docker Compose**
- **Poetry** — управление зависимостями
- **Linters:** Flake8, Black, isort

---

## Структура проекта

    ├── dags/                    # DAG Airflow
    │   └── habr_career_etl.py
    ├── src/
    │   ├── api/                 # Парсер (JSON)
    │   ├── database/            # Работа с БД
    │   ├── models/              # Модели Vacancy и Employer
    │   └── notifications/       # Telegram-уведомления
    │   └── logging_config.py
    ├── logs/
    ├── .env
    ├── docker-compose.yml
    ├── pyproject.toml
    ├── run_etl.py               # Локальный запуск
    └── README.md

---

## Основные сущности

- **Vacancy** — вакансия (id, название, url, зарплата, регион, компания)
- **Employer** — работодатель (id, название, url)
- **HabrCareerVacanciesSource** — источник данных (JSON API)
- **HabrCareerDBCreator** — создание и заполнение базы
- **HabrCareerDBWriter** — запись данных (компании + вакансии)

---

## Запуск проекта

### Через Docker

```
1. Клонировать репозиторий
    git clone https://github.com/simbarilion/HH_Vacancy_etl_airflow
    cd HH_Vacancy_etl_airflow

2. Создать .env файл и настроить переменные окружения на основе примера (env.example):
    DB_HOST=db
    DB_PORT=5432
    DB_USER=postgres
    DB_PASSWORD=your_postgresql_password_here
    DB_NAME=hc_vacancies_employers
    TELEGRAM_BOT_TOKEN=8285072430:AAF8oiYSySPkdVRJagBHNGxx1jYVK2NWirw
    TELEGRAM_CHAT_ID=your_telegram_chat_id_here
    ALLOWED_HOSTS=localhost,127.0.0.1,0.0.0.0
    AIRFLOW_SECRET_KEY=set_your_random_key

3. Добавить GitHub Secrets
    - В репозитории - Settings - Secrets and variables - Actions должны быть добавлены:
        
    | Secret                    | Назначение                                           |
    |---------------------------|------------------------------------------------------|
    | `DB_PASSWORD`             | Твой пароль для подключения к базе данных PostgreSQL |
    | `AIRFLOW_SECRET_KEY`      | Секретный ключ для запуска AIRFLOW                   |

4. Запустить базу данных
    docker compose up db -d

5. Создать базу Airflow
    docker compose exec db psql -U postgres
    CREATE DATABASE airflow;
    GRANT ALL PRIVILEGES ON DATABASE airflow TO postgres;
    \q
    docker compose run --rm --entrypoint "airflow db init" airflow-webserver

6. Создать пользователя admin в Airflow (если не создан)
    docker compose run --rm --entrypoint "" airflow-webserver airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin  

7. Запустить Airflow
    docker compose up -d

8. Открыть Airflow UI
    Перейти в браузере: http://localhost:8080
    Логин: admin
    Пароль: admin
```

### Локальный запуск (без Docker)

```
1. Клонировать репозиторий
    git clone https://github.com/simbarilion/HH_Vacancy_etl_airflow
    cd HH_Vacancy_etl_airflow
    
2. 2. Установить зависимости
  - Через Poetry:
    poetry install
    poetry shell
  - Через pip:
    pip install -r requirements.txt

3. Создать .env файл и настроить переменные окружения на основе примера (env.example):
    DB_HOST=db
    DB_PORT=5432
    DB_USER=postgres
    DB_PASSWORD=your_postgresql_password_here
    DB_NAME=hc_vacancies_employers
    TELEGRAM_BOT_TOKEN=8285072430:AAF8oiYSySPkdVRJagBHNGxx1jYVK2NWirw
    TELEGRAM_CHAT_ID=your_telegram_chat_id_here
    ALLOWED_HOSTS=localhost,127.0.0.1,0.0.0.0
    AIRFLOW_SECRET_KEY=set_your_random_key

3. Запустить приложение
    python run_etl.py
```
---

## Архитектурные решения

- Разделение ответственности: Extract → Load
- Асинхронный сбор данных с контролем параллельности ('asyncio.Semaphore')
- Использование JSON API вместо HTML-парсинга (более стабильный подход)
- Одна транзакция на подготовку таблиц, отдельная — на запись данных
- Логирование, совместимое с Airflow UI
- Конфигурация через '.env'
- Контейнеризация всего стека (PostgreSQL + Airflow)

---

## Возможные улучшения

- Добавление поддержки нескольких ключевых слов одновременно
- Автоматический анализ трендов зарплат
- Добавление тестов
- CI/CD через GitHub Actions
- Поддержка прокси и ротации User-Agent для снижения риска блокировки

### Автор

Надежда Попова

Python Developer

📧 nadezhdapopova13@yandex.ru

🔗 GitHub: simbarilion

