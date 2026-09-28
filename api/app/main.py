"""HTTP-слой API: lifespan с пулами, health, превращение сбоев зависимостей в 503.

Запуск — gunicorn + UvicornWorker (см. api/Dockerfile). Всё из lifespan создаётся
в КАЖДОМ воркере отдельно, уже после fork'а.
"""

import asyncio
import logging
import os
import socket
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import asyncpg
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from redis.exceptions import RedisError

from app.config import Settings, get_settings
from app.db import create_db_pool, db_connection
from app.logging_config import setup_logging
from app.redis_client import create_redis_client, create_redis_pool
from app.request_id import RequestIdMiddleware

log = logging.getLogger(__name__)

HOSTNAME = socket.gethostname()


def error(status: int, detail: str, reason: str, retry_after: int | None = None) -> JSONResponse:
    # reason — машиночитаемая причина: по одному коду клиент не поймёт, что делать.
    headers = {"Retry-After": str(retry_after)} if retry_after is not None else None
    return JSONResponse(
        status_code=status, content={"detail": detail, "reason": reason}, headers=headers
    )


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or get_settings()
    setup_logging(settings.log_level)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        redis_pool = create_redis_pool(settings)
        # Если Postgres недоступен при старте, воркер всё равно поднимается:
        # health честно отвечает 503, а пул создаётся при первой возможности.
        # Иначе gunicorn бесконечно перезапускал бы воркеры, и мы теряли бы
        # даже ответы, которым база не нужна (статусы, кэшируемые данные).
        app.state.settings = settings
        app.state.redis = create_redis_client(redis_pool)
        app.state.db = None
        app.state.db_lock = asyncio.Lock()
        try:
            await ensure_db_pool(app)
        except DatabaseUnavailable:
            pass  # уже залогировано; повторим при первом запросе к БД
        log.info("worker_started", extra={"pid": os.getpid()})
        try:
            yield
        finally:
            if app.state.db is not None:
                await app.state.db.close()
            await redis_pool.disconnect()
            log.info("worker_stopped", extra={"pid": os.getpid()})

    app = FastAPI(title="ticket-rush", lifespan=lifespan)
    app.add_middleware(RequestIdMiddleware)

    # --- Сбои зависимостей -> 503, а не 500 ---------------------------------
    # Недоступный Redis/Postgres или исчерпанный пул — это временная
    # недоступность, а не ошибка в коде: клиенту 503 + Retry-After, повтор уместен.

    @app.exception_handler(RedisError)
    async def redis_unavailable(request: Request, exc: RedisError) -> JSONResponse:
        log.error("redis_unavailable", extra={"error": repr(exc)})
        return error(503, "Хранилище временно недоступно", "redis_unavailable", 2)

    @app.exception_handler(DatabaseUnavailable)
    async def db_unavailable(request: Request, exc: DatabaseUnavailable) -> JSONResponse:
        log.error("database_unavailable", extra={"error": repr(exc.__cause__ or exc)})
        return error(503, "База данных временно недоступна", "database_unavailable", 2)

    @app.get("/api/health")
    async def health(request: Request) -> JSONResponse:
        # Обе зависимости проверяются параллельно: health вызывают часто (Docker
        # раз в 15 с на каждую реплику), и он не должен ждать их по очереди.
        redis_ok, db_ok = await asyncio.gather(
            check_redis(request.app), check_db(request.app)
        )
        body = {
            "status": "ok" if redis_ok and db_ok else "error",
            "redis": "ok" if redis_ok else "down",
            "database": "ok" if db_ok else "down",
            "hostname": HOSTNAME,
        }
        # 503, а не 200 с "down" внутри: HEALTHCHECK Docker и мониторинг смотрят
        # на код ответа.
        return JSONResponse(status_code=200 if redis_ok and db_ok else 503, content=body)

    @app.get("/api/debug/instance-info")
    async def instance_info() -> dict:
        return {"hostname": HOSTNAME, "pid": os.getpid()}

    return app


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
async def db(request: Request) -> AsyncIterator[asyncpg.Connection]:
    """Соединение с БД для обработчика; любые сбои связи -> DatabaseUnavailable."""
    try:
        pool = await ensure_db_pool(request.app)
        async with db_connection(pool, request.app.state.settings) as connection:
            yield connection
    except DatabaseUnavailable:
        raise
    except DB_CONNECTION_ERRORS as exc:
        raise DatabaseUnavailable from exc


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


# Точка входа для `gunicorn app.main:app`. Соединений здесь не открывается —
# всё тяжёлое происходит в lifespan, уже внутри каждого воркера.
app = create_app()
