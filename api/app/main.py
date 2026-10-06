"""Сборка приложения: lifespan с пулами, маршруты, превращение сбоев в 503.

Запуск — gunicorn + UvicornWorker (см. api/Dockerfile). Всё из lifespan создаётся
в КАЖДОМ воркере отдельно, уже после fork'а.
"""

import asyncio
import logging
import os
import socket
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from redis.exceptions import RedisError

from app import catalog, holds_routes, orders_routes, queue_routes
from app.admitter import Admitter
from app.config import Settings, get_settings
from app.deps import DatabaseUnavailable, check_db, check_redis, ensure_db_pool, error
from app.logging_config import setup_logging
from app.payments import FakePaymentProvider
from app.queue_routes import EventScheduleCache
from app.realtime import (
    ConnectionManager,
    RedisSubscriber,
    SeatChangeBatcher,
    route_by_event,
    route_to_session,
)
from app.redis_client import create_redis_client, create_redis_pool
from app.request_id import RequestIdMiddleware
from app.seats import CHANGES_CHANNEL, SeatCatalog, SeatStore
from app.ticket_jobs import ORDERS_CHANNEL
from app.waiting_room import STATE_CHANNEL, WaitingRoom

log = logging.getLogger(__name__)

HOSTNAME = socket.gethostname()


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or get_settings()
    setup_logging(settings.log_level)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        redis_pool = create_redis_pool(settings)
        app.state.settings = settings
        app.state.redis = create_redis_client(redis_pool)
        app.state.db = None
        app.state.db_lock = asyncio.Lock()
        # Если Postgres недоступен при старте, воркер всё равно поднимается:
        # health честно отвечает 503, а пул создаётся при первой возможности.
        # Иначе gunicorn бесконечно перезапускал бы воркеры.
        try:
            await ensure_db_pool(app)
        except DatabaseUnavailable:
            pass  # уже залогировано; повторим при первом запросе к БД

        app.state.room = WaitingRoom(app.state.redis)
        app.state.seats = SeatStore(app.state.redis)
        app.state.seat_catalog = SeatCatalog()
        app.state.payments = FakePaymentProvider(
            app.state.redis, settings.fake_payment_latency_ms, settings.fake_payment_failure_rate
        )
        app.state.schedule = EventScheduleCache()
        app.state.manager = ConnectionManager()
        # Одна подписка воркера на все каналы: состояние очередей и изменения мест
        # -> зрителям концерта, личные уведомления о заказах -> вкладкам сессии.
        route = route_by_event(app.state.manager)
        subscriber = RedisSubscriber(
            app.state.redis,
            {
                STATE_CHANNEL: route,
                CHANGES_CHANNEL: SeatChangeBatcher(
                    app.state.manager, settings.seat_changes_batch_ms / 1000
                ).handle,
                ORDERS_CHANNEL: route_to_session(app.state.manager),
            },
        )
        await subscriber.start()
        app.state.admitter = Admitter(app, app.state.room, app.state.seats, settings)
        if settings.admitter_enabled:
            app.state.admitter.start()

        log.info("worker_started", extra={"pid": os.getpid()})
        try:
            yield
        finally:
            await app.state.admitter.stop()
            await subscriber.stop()
            if app.state.db is not None:
                await app.state.db.close()
            await redis_pool.disconnect()
            log.info("worker_stopped", extra={"pid": os.getpid()})

    app = FastAPI(title="ticket-rush", lifespan=lifespan)
    app.add_middleware(RequestIdMiddleware)
    app.include_router(catalog.router)
    app.include_router(queue_routes.router)
    app.include_router(holds_routes.router)
    app.include_router(orders_routes.router)

    # --- Сбои зависимостей -> 503, а не 500 ---------------------------------
    # Недоступный Redis/Postgres или исчерпанный пул — временная недоступность,
    # а не ошибка в коде: клиенту 503 + Retry-After, повтор уместен.

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
        # Параллельно: health вызывают часто, и он не должен ждать зависимости
        # по очереди.
        redis_ok, db_ok = await asyncio.gather(
            check_redis(request.app), check_db(request.app)
        )
        body = {
            "status": "ok" if redis_ok and db_ok else "error",
            "redis": "ok" if redis_ok else "down",
            "database": "ok" if db_ok else "down",
            "hostname": HOSTNAME,
        }
        # 503, а не 200 с "down" внутри: HEALTHCHECK и мониторинг смотрят на код.
        return JSONResponse(status_code=200 if redis_ok and db_ok else 503, content=body)

    @app.get("/api/debug/instance-info")
    async def instance_info(request: Request) -> dict:
        return {
            "hostname": HOSTNAME,
            "pid": os.getpid(),
            "admitter_leader": request.app.state.admitter.is_leader,
            "ws_clients": request.app.state.manager.count(),
        }

    return app


# Модульной переменной app нет намеренно: gunicorn вызывает фабрику сам
# (`gunicorn "app.main:create_app()"`). Иначе любой import app.main читал бы
# настройки — и падал бы без SESSION_SECRET даже там, где он не нужен.
