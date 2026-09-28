"""Health и поведение при сбоях зависимостей."""

import time

from conftest import running_app


async def test_health_ok(settings, db, redis):
    async with running_app(settings) as client:
        response = await client.get("/api/health")
        assert response.status_code == 200
        assert response.json() | {"hostname": None} == {
            "status": "ok",
            "redis": "ok",
            "database": "ok",
            "hostname": None,
        }


async def test_app_starts_and_reports_when_database_is_down(settings, redis):
    # Порт, на котором никого нет: база "лежит". Воркер должен стартовать
    # (иначе gunicorn перезапускал бы его по кругу) и честно отвечать 503.
    settings = settings.model_copy(
        update={"database_url": "postgresql://ticket:ticket@127.0.0.1:1/ticket"}
    )
    async with running_app(settings) as client:
        response = await client.get("/api/health")
        assert response.status_code == 503
        assert response.json()["database"] == "down"
        assert response.json()["redis"] == "ok"


async def test_redis_down_reported(settings, db):
    settings = settings.model_copy(
        update={
            "redis_url": "redis://127.0.0.1:1/0",
            "redis_connect_timeout_seconds": 0.2,
            "redis_socket_timeout_seconds": 0.2,
        }
    )
    async with running_app(settings) as client:
        response = await client.get("/api/health")
        assert response.status_code == 503
        assert response.json()["redis"] == "down"
        assert response.json()["database"] == "ok"


async def test_exhausted_db_pool_fails_fast_instead_of_hanging(settings, db, redis):
    # Пул на 1 соединение, и его держит "другой запрос". Следующий должен
    # получить отказ через db_acquire_timeout, а не ждать вечно (поведение
    # asyncpg по умолчанию).
    settings = settings.model_copy(
        update={"db_pool_min": 1, "db_pool_max": 1, "db_acquire_timeout_seconds": 0.3}
    )
    async with running_app(settings) as client:
        busy = await client.app.state.db.acquire()
        try:
            started = time.monotonic()
            response = await client.get("/api/health")
            assert response.status_code == 503
            assert response.json()["database"] == "down"
            assert time.monotonic() - started < 2
        finally:
            await client.app.state.db.release(busy)

        assert (await client.get("/api/health")).status_code == 200
