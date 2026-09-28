"""Общие фикстуры: НАСТОЯЩИЕ PostgreSQL и Redis в контейнерах (Testcontainers).

Моки проверяли бы код против нашего же представления о базе. А важное здесь —
семантика настоящих систем: транзакции, ограничения UNIQUE/CHECK, поведение пула.
"""

from contextlib import asynccontextmanager
from pathlib import Path

import asyncpg
import httpx
import pytest
from alembic import command
from alembic.config import Config

from app.config import Settings
from app.main import create_app
from app.redis_client import create_redis_client, create_redis_pool

try:
    from testcontainers.community.postgres import PostgresContainer
    from testcontainers.community.redis import RedisContainer
except ImportError:  # старые версии testcontainers держали модули в корне пакета
    from testcontainers.postgres import PostgresContainer
    from testcontainers.redis import RedisContainer

API_DIR = Path(__file__).resolve().parents[1]


@pytest.fixture(scope="session")
def postgres_container():
    # driver=None: адрес вида postgresql://..., без драйвера psycopg2 в схеме.
    with PostgresContainer(
        "postgres:17-alpine", username="ticket", password="ticket", dbname="ticket", driver=None
    ) as container:
        yield container


@pytest.fixture(scope="session")
def redis_container():
    with RedisContainer("redis:7-alpine") as container:
        yield container


@pytest.fixture(scope="session")
def database_url(postgres_container) -> str:
    host = postgres_container.get_container_host_ip()
    port = postgres_container.get_exposed_port(5432)
    return f"postgresql://ticket:ticket@{host}:{port}/ticket"


@pytest.fixture(scope="session")
def redis_url(redis_container) -> str:
    host = redis_container.get_container_host_ip()
    port = redis_container.get_exposed_port(6379)
    return f"redis://{host}:{port}/0"


def alembic_config(database_url: str) -> Config:
    config = Config(str(API_DIR / "alembic.ini"))
    config.set_main_option("script_location", str(API_DIR / "migrations"))
    config.set_main_option("sqlalchemy.url", database_url)
    return config


@pytest.fixture(scope="session")
def migrated(database_url) -> str:
    # Синхронная фикстура: env.py alembic сам запускает asyncio.run(), а внутри
    # уже работающего event loop pytest-asyncio это запрещено.
    command.upgrade(alembic_config(database_url), "head")
    return database_url


@pytest.fixture
def settings(migrated, redis_url) -> Settings:
    return Settings(
        database_url=migrated,
        redis_url=redis_url,
        db_pool_min=1,
        db_pool_max=5,
        db_acquire_timeout_seconds=2,
        session_secret="test-secret-" + "x" * 32,
        # ASGI-клиент ходит по http://, а Secure-cookie браузер отправляет только
        # по HTTPS — httpx ведёт себя так же.
        session_cookie_secure=False,
        # Тики впускателя тесты вызывают сами.
        admitter_enabled=False,
    )


@pytest.fixture
async def db(settings):
    """Соединение с чистой базой: таблицы очищаются перед каждым тестом."""
    connection = await asyncpg.connect(settings.database_url)
    await connection.execute("TRUNCATE events RESTART IDENTITY CASCADE")
    yield connection
    await connection.close()


@pytest.fixture
async def redis(settings):
    pool = create_redis_pool(settings)
    client = create_redis_client(pool)
    await client.flushdb()
    yield client
    await pool.disconnect()


@asynccontextmanager
async def running_app(settings: Settings):
    """Приложение с настоящим lifespan и httpx-клиент к нему через ASGI, без сети."""
    app = create_app(settings)
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            client.app = app
            yield client
