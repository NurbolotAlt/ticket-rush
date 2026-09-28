"""Запуск миграций alembic через asyncpg — тот же драйвер, что у приложения.

Миграции пишутся на чистом SQL (op.execute), без моделей SQLAlchemy: схема базы
видна как есть, и нет второго описания таблиц, которое может разойтись с кодом.

Адрес БД не хранится в alembic.ini — он берётся из DATABASE_URL, чтобы пароль не
попал в git. (alembic.ini — только ASCII: на Windows configparser читает его в
кодировке системы, и кириллица в комментарии роняет запуск.)
"""

import asyncio
from logging.config import fileConfig

from alembic import context
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

from app.config import get_settings

config = context.config
if config.config_file_name is not None:
    fileConfig(config.config_file_name)


def database_url() -> str:
    # Тесты передают адрес контейнера явно; иначе — из DATABASE_URL.
    url = config.get_main_option("sqlalchemy.url") or get_settings().database_url
    # SQLAlchemy нужно имя драйвера в схеме URL.
    return url.replace("postgresql://", "postgresql+asyncpg://", 1)


def run_migrations(connection) -> None:
    context.configure(connection=connection, target_metadata=None)
    with context.begin_transaction():
        context.run_migrations()


async def run_async_migrations() -> None:
    # NullPool: миграции — разовый процесс, держать пул соединений незачем.
    engine = create_async_engine(database_url(), poolclass=NullPool)
    async with engine.connect() as connection:
        await connection.run_sync(run_migrations)
    await engine.dispose()


if context.is_offline_mode():
    raise SystemExit("Offline-режим не используется: миграции применяются к живой БД")

asyncio.run(run_async_migrations())
