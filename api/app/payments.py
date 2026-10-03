"""Имитация платёжного провайдера (вместо Stripe/ЮKassa).

Ведёт себя как настоящий в главном: отвечает с задержкой, иногда отказывает и
ИДЕМПОТЕНТЕН по ключу — повторное списание по тому же заказу не списывает
второй раз, а возвращает результат первого. Реальные провайдеры принимают
ключ идемпотентности именно для этого: сеть может оборваться ПОСЛЕ списания, и
клиент (наш сервер) повторит запрос, не зная, прошёл ли первый.

Журнал платежей — в Redis (это "внешняя система", не наша база).
"""

import asyncio
import random

from redis.asyncio import Redis

LEDGER = "fakepay:charges"


class PaymentDeclined(Exception):
    pass


class FakePaymentProvider:
    def __init__(self, redis: Redis, latency_ms: int, failure_rate: float) -> None:
        self._redis = redis
        self._latency = latency_ms / 1000
        self._failure_rate = failure_rate

    async def charge(self, order_id: int, amount_cents: int) -> None:
        await asyncio.sleep(self._latency * random.uniform(0.5, 1.5))
        if await self._redis.hexists(LEDGER, order_id):
            return  # уже списано этим ключом — повтор, второй раз не списываем
        if random.random() < self._failure_rate:
            raise PaymentDeclined("Банк отклонил платёж")
        await self._redis.hsetnx(LEDGER, order_id, amount_cents)

    async def refund(self, order_id: int) -> bool:
        """True — деньги были списаны и возвращены."""
        return bool(await self._redis.hdel(LEDGER, order_id))

    async def charged(self, order_id: int) -> bool:
        return bool(await self._redis.hexists(LEDGER, order_id))
