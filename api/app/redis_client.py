"""Пул соединений к Redis — один на воркер, создаётся в lifespan.

BlockingConnectionPool, а не обычный ConnectionPool: обычный при исчерпании
СРАЗУ бросает MaxConnectionsError, блокирующий — ждёт свободное соединение до
timeout. В проекте compute-service это разница между 493 ошибками и нулём на
тесте с 1000 одновременных клиентов.
"""

from redis.asyncio import BlockingConnectionPool, Redis

from app.config import Settings


def create_redis_pool(settings: Settings) -> BlockingConnectionPool:
    return BlockingConnectionPool.from_url(
        settings.redis_url,
        max_connections=settings.redis_max_connections,
        timeout=settings.redis_pool_timeout_seconds,
        # Без таймаутов сокета зависший Redis подвесил бы запросы навсегда.
        socket_timeout=settings.redis_socket_timeout_seconds,
        socket_connect_timeout=settings.redis_connect_timeout_seconds,
        socket_keepalive=True,
        decode_responses=True,
    )


def create_redis_client(pool: BlockingConnectionPool) -> Redis:
    return Redis(connection_pool=pool)
