from functools import lru_cache

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Все параметры — из переменных окружения: один образ и локально, и в CI."""

    # --- PostgreSQL ---
    database_url: str = "postgresql://ticket:ticket@localhost:5432/ticket"
    # Размер пула соединений к БД на ОДИН gunicorn-воркер. Соединение с Postgres
    # дорогое (отдельный процесс на сервере, ~10 МБ памяти), а max_connections
    # у сервера конечен (100). Считать нужно по всей системе:
    # реплики × воркеры × db_pool_max + служебные соединения < max_connections.
    db_pool_min: int = 2
    db_pool_max: int = 10
    # Пул asyncpg при нехватке соединений ЖДЁТ свободное — но по умолчанию вечно.
    # Ограничиваем: не дождались за 5 с — 503, а не зависший запрос.
    db_acquire_timeout_seconds: float = 5.0
    # Потолок на один запрос к БД. Наши запросы — миллисекунды; секунды — авария.
    db_command_timeout_seconds: float = 5.0
    db_connect_timeout_seconds: float = 3.0

    # --- Redis ---
    redis_url: str = "redis://localhost:6379/0"
    redis_max_connections: int = 50
    redis_pool_timeout_seconds: float = 5.0
    redis_socket_timeout_seconds: float = 5.0
    redis_connect_timeout_seconds: float = 2.0

    log_level: str = "INFO"

    # --- Наполнение зала (python -m app.seed) ---
    seed_event_slug: str = "rush-live-2026"
    seed_sections: str = "A,B,C,D"
    seed_rows: int = 20
    seed_seats_per_row: int = 25
    # Через сколько минут после наполнения откроются продажи (этап 3).
    seed_sale_opens_in_minutes: int = 10


@lru_cache
def get_settings() -> Settings:
    return Settings()
