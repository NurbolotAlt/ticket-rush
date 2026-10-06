"""Рассылка по WebSocket: порядок сообщений и изоляция медленных клиентов."""

import asyncio
import json
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


async def test_personal_message_reaches_only_that_session():
    manager = ConnectionManager()
    mine_tab1, mine_tab2, stranger = FakeWebSocket(), FakeWebSocket(), FakeWebSocket()
    manager.add(1, mine_tab1, "alice")
    manager.add(1, mine_tab2, "alice")  # вторая вкладка той же сессии
    manager.add(1, stranger, "bob")

    manager.send_to_session("alice", "ваши билеты готовы")
    await mine_tab1.wait_received(1)
    await mine_tab2.wait_received(1)
    await asyncio.sleep(0.05)

    assert stranger.received == []


async def test_batcher_merges_contiguous_versions_and_keeps_last_status():
    from app.realtime import SeatChangeBatcher

    manager = ConnectionManager()
    ws = FakeWebSocket()
    manager.add(1, ws)
    batcher = SeatChangeBatcher(manager, window_seconds=0.05)

    batcher.handle('{"event_id":1,"version":1,"changes":[[10,"held"]]}')
    batcher.handle('{"event_id":1,"version":2,"changes":[[11,"held"]]}')
    batcher.handle('{"event_id":1,"version":3,"changes":[[10,"sold"]]}')
    # версия 4 потерялась — 5 должна уйти отдельным сообщением
    batcher.handle('{"event_id":1,"version":5,"changes":[[12,"held"]]}')
    await ws.wait_received(2)

    first, second = (json.loads(m) for m in ws.received)
    assert (first["from_version"], first["version"]) == (1, 3)
    assert sorted(first["changes"]) == [[10, "sold"], [11, "held"]]  # у места 10 — последний
    assert (second["from_version"], second["version"]) == (5, 5)
