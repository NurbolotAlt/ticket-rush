"""Выбор лидера через Redis: ровно один процесс на кластер выполняет работу.

Лидерство — это лок с коротким TTL (аренда), который лидер постоянно продлевает.
Умер лидер или завис — аренда истекает, и её берёт другой кандидат. Никакой
координации между кандидатами не нужно: все просто периодически пытаются
SET NX, и выигрывает ровно один.

Важно: аренда — не гарантия. Лидер, уснувший дольше TTL (пауза процесса, сеть),
может не знать, что его сменили. Поэтому всё, что делает лидер, дополнительно
защищено проверкой "я ещё лидер?" внутри самой операции (см. _ADMIT в
waiting_room.py) — это называется fencing.
"""

from redis.asyncio import Redis

# Продлить/снять аренду можно только СВОЮ: иначе процесс, чья аренда истекла и
# перешла другому, продлил бы или удалил чужую.
_RENEW = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('PEXPIRE', KEYS[1], ARGV[2])
end
return 0
"""
_RELEASE = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('DEL', KEYS[1])
end
return 0
"""


class LeaderLease:
    def __init__(self, redis: Redis, key: str, identity: str, ttl_ms: int) -> None:
        self._redis = redis
        self.key = key
        self.identity = identity
        self._ttl_ms = ttl_ms
        self._renew = redis.register_script(_RENEW)
        self._release = redis.register_script(_RELEASE)

    async def acquire(self) -> bool:
        return bool(await self._redis.set(self.key, self.identity, nx=True, px=self._ttl_ms))

    async def renew(self) -> bool:
        return bool(await self._renew(keys=[self.key], args=[self.identity, self._ttl_ms]))

    async def release(self) -> bool:
        return bool(await self._release(keys=[self.key], args=[self.identity]))
