"""Пул соединений к PostgreSQL (asyncpg).

Один пул на gunicorn-воркер, создаётся в lifespan — после fork'а воркера (иначе
воркеры унаследовали бы копии одних и тех же TCP-соединений).

Почему asyncpg напрямую, без ORM: запросы у нас простые и горячие, и хочется
видеть ровно тот SQL, который уходит в базу. SQLAlchemy используется только
alembic'ом для миграций.
"""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import asyncpg

from app.config import Settings


async def create_db_pool(settings: Settings) -> asyncpg.Pool:
    return await asyncpg.create_pool(
        settings.database_url,
        min_size=settings.db_pool_min,
        max_size=settings.db_pool_max,
        # Таймаут установки соединения. По умолчанию у asyncpg 60 с — при лежащей
        # базе запрос висел бы минуту, прежде чем получить 503.
        timeout=settings.db_connect_timeout_seconds,
        # Таймаут на КАЖДЫЙ запрос по умолчанию: зависший запрос не держит
        # соединение из пула бесконечно (asyncpg отменяет его на сервере).
        command_timeout=settings.db_command_timeout_seconds,
        # Простаивающие соединения закрываются через 5 минут: после всплеска
        # нагрузки пул сжимается до min_size и не держит лишние процессы Postgres.
        max_inactive_connection_lifetime=300,
    )


@asynccontextmanager
async def db_connection(
    pool: asyncpg.Pool, settings: Settings
) -> AsyncIterator[asyncpg.Connection]:
    """Соединение из пула с ограниченным ожиданием.

    Методы pool.fetch()/fetchval() сами берут соединение, но БЕЗ таймаута на
    ожидание: при исчерпанном пуле запрос висел бы вечно (проверено
    экспериментом). Поэтому весь код берёт соединения только через эту функцию.
    TimeoutError при исчерпании превращается в 503 в main.py.
    """
    async with pool.acquire(timeout=settings.db_acquire_timeout_seconds) as connection:
        yield connection
