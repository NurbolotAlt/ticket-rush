"""Rate limit в Redis: fixed window через INCR + EXPIRE NX (из compute-service).

Счётчик в Redis, а не в памяти процесса: реплик и воркеров несколько, и лимит
должен быть общим, иначе реальный лимит умножался бы на их число.
"""

from dataclasses import dataclass

from redis.asyncio import Redis


@dataclass(frozen=True)
class RateLimitResult:
    allowed: bool
    retry_after: int


async def check_rate_limit(
    redis: Redis, key: str, limit: int, window_seconds: int
) -> RateLimitResult:
    # INCR и EXPIRE — в одной транзакции MULTI/EXEC: процесс не может упасть между
    # ними и оставить ключ без TTL (вечная блокировка). EXPIRE NX — окно считается
    # от ПЕРВОГО запроса и не продлевается каждым следующим.
    async with redis.pipeline(transaction=True) as pipe:
        pipe.incr(key)
        pipe.expire(key, window_seconds, nx=True)
        pipe.ttl(key)
        current, _, ttl = await pipe.execute()
    return RateLimitResult(allowed=current <= limit, retry_after=max(ttl, 1))
