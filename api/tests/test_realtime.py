"""Рассылка по WebSocket: порядок сообщений и изоляция медленных клиентов."""

import asyncio
import random

from app.realtime import OUTBOX_LIMIT, ConnectionManager


class FakeWebSocket:
    def __init__(self, delay=lambda: 0.0, hang=False):
        self.received: list[str] = []
        self.closed_with = None
        self._changed = asyncio.Condition()
        self._delay = delay
        self._hang = hang

    async def send_text(self, message: str) -> None:
        if self._hang:
            await asyncio.sleep(3600)
        await asyncio.sleep(self._delay())
        async with self._changed:
            self.received.append(message)
            self._changed.notify_all()

    async def wait_received(self, count: int) -> None:
        async with asyncio.timeout(5), self._changed:
            await self._changed.wait_for(lambda: len(self.received) >= count)

    async def close(self, code: int = 1000) -> None:
        self.closed_with = code


async def test_messages_arrive_in_order_despite_random_send_delays():
    # Случайные задержки отправки: при задаче на каждое сообщение порядок бы
    # перемешался. Очередь и один отправитель на клиента его сохраняют.
    manager = ConnectionManager()
    ws = FakeWebSocket(delay=lambda: random.uniform(0, 0.005))
    manager.add(1, ws)

    for version in range(1, 51):
        manager.broadcast_nowait(1, str(version))
    await ws.wait_received(50)

    assert ws.received == [str(v) for v in range(1, 51)]


async def test_stuck_client_is_dropped_without_delaying_others():
    manager = ConnectionManager()
    fast, stuck = FakeWebSocket(), FakeWebSocket(hang=True)
    manager.add(1, fast)
    manager.add(1, stuck)

    for i in range(OUTBOX_LIMIT + 5):
        manager.broadcast_nowait(1, str(i))
        # Сообщения приходят из подписки по одному, между ними event loop успевает
        # отправить уже накопленное — как и здесь.
        await asyncio.sleep(0)
    await fast.wait_received(OUTBOX_LIMIT + 5)
    await asyncio.sleep(0.05)

    # Быстрый получил всё; зависший отключён (1013 — "переподключитесь позже").
    assert manager.count(1) == 1
    assert stuck.closed_with == 1013
