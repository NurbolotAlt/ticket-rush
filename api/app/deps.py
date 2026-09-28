"""Общее для всех обработчиков: доступ к БД и Redis, ответы-ошибки, кэш-заголовки."""

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import asyncpg
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response
from redis.exceptions import RedisError

from app.db import create_db_pool, db_connection

log = logging.getLogger(__name__)


class DatabaseUnavailable(Exception):
    """Postgres недоступен, пул не создан или исчерпан дольше таймаута."""


# Ошибки СВЯЗИ с базой (а не ошибки в запросе вроде нарушения UNIQUE — те должны
# обрабатываться бизнес-логикой): сеть, разорванное соединение, таймаут пула.
DB_CONNECTION_ERRORS = (
    OSError,
    TimeoutError,
    asyncpg.PostgresConnectionError,
    asyncpg.InterfaceError,
)


def error(status: int, detail: str, reason: str, retry_after: int | None = None) -> JSONResponse:
    # reason — машиночитаемая причина: по одному коду клиент не поймёт, что делать.
    headers = {"Retry-After": str(retry_after)} if retry_after is not None else None
    return JSONResponse(
        status_code=status, content={"detail": detail, "reason": reason}, headers=headers
    )


def client_ip(request: Request) -> str:
    """IP клиента из X-Real-IP.

    За nginx request.client.host — адрес контейнера nginx, одинаковый для всех.
    X-Real-IP nginx ПЕРЕЗАПИСЫВАЕТ адресом соединения, а API недоступен снаружи в
    обход nginx — поэтому заголовку можно доверять.
    """
    real_ip = request.headers.get("x-real-ip")
    if real_ip:
        return real_ip.strip()
    return request.client.host if request.client else "unknown"


def cacheable(response: Response, seconds: int) -> Response:
    """Разрешить кэширование ответа на `seconds` секунд.

    Решает API, а не nginx: только API знает, насколько данные могут устареть без
    вреда (каталог — секунды, схема зала — секунду). nginx кэширует по этому
    заголовку, браузер — тоже.
    """
    response.headers["Cache-Control"] = f"public, max-age={seconds}"
    return response


async def ensure_db_pool(app: FastAPI) -> asyncpg.Pool:
    if app.state.db is not None:
        return app.state.db
    # Лок, чтобы сотня одновременных запросов при недоступной БД не создавала
    # сотню пулов наперегонки.
    async with app.state.db_lock:
        if app.state.db is None:
            try:
                app.state.db = await create_db_pool(app.state.settings)
            except (OSError, asyncpg.PostgresError, TimeoutError) as exc:
                log.warning("database_pool_create_failed", extra={"error": repr(exc)})
                raise DatabaseUnavailable from exc
    return app.state.db


@asynccontextmanager
async def db_for_app(app: FastAPI) -> AsyncIterator[asyncpg.Connection]:
    """Соединение с БД; любые сбои связи -> DatabaseUnavailable.

    По приложению, а не по запросу: нужно и фоновым задачам (впускатель), у
    которых HTTP-запроса нет.
    """
    try:
        pool = await ensure_db_pool(app)
        async with db_connection(pool, app.state.settings) as connection:
            yield connection
    except DatabaseUnavailable:
        raise
    except DB_CONNECTION_ERRORS as exc:
        raise DatabaseUnavailable from exc


def db(request: Request):
    """Соединение с БД для обработчика HTTP-запроса."""
    return db_for_app(request.app)


async def check_redis(app: FastAPI) -> bool:
    try:
        await app.state.redis.ping()
        return True
    except RedisError:
        return False


async def check_db(app: FastAPI) -> bool:
    try:
        pool = await ensure_db_pool(app)
        async with db_connection(pool, app.state.settings) as connection:
            await connection.fetchval("SELECT 1")
        return True
    except (DatabaseUnavailable, *DB_CONNECTION_ERRORS, asyncpg.PostgresError):
        return False
