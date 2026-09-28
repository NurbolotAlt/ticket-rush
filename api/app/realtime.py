"""Живые обновления: Redis Pub/Sub между воркерами + WebSocket до браузеров.

Каждый воркер держит ОДНУ подписку на Redis (а не по одной на зрителя) и раздаёт
полученное своим WebSocket-клиентам. Число соединений с Redis = число воркеров,
сколько бы ни было зрителей. (Паттерн из compute-service.)
"""

import asyncio
import json
import logging
from collections import defaultdict
from collections.abc import Callable

from fastapi import WebSocket
from redis.asyncio import Redis
from redis.exceptions import RedisError

log = logging.getLogger(__name__)

# Медленный клиент (плохая сеть) не должен задерживать рассылку остальным.
SEND_TIMEOUT_SECONDS = 2.0
RECONNECT_DELAY_SECONDS = 1.0


class ConnectionManager:
    """WebSocket-клиенты ОДНОГО воркера, сгруппированные по концерту."""

    def __init__(self) -> None:
        self._by_event: dict[int, set[WebSocket]] = defaultdict(set)
        # asyncio держит на задачи только слабые ссылки — без этого множества
        # незавершённую рассылку мог бы собрать сборщик мусора.
        self._tasks: set[asyncio.Task] = set()

    def count(self, event_id: int | None = None) -> int:
        if event_id is not None:
            return len(self._by_event.get(event_id, ()))
        return sum(len(clients) for clients in self._by_event.values())

    def add(self, event_id: int, websocket: WebSocket) -> None:
        self._by_event[event_id].add(websocket)

    def remove(self, event_id: int, websocket: WebSocket) -> None:
        clients = self._by_event.get(event_id)
        if clients is not None:
            clients.discard(websocket)
            if not clients:
                del self._by_event[event_id]

    def broadcast_nowait(self, event_id: int, message: str) -> None:
        """Для обработчика подписки: запустить рассылку и сразу вернуться."""
        targets = list(self._by_event.get(event_id, ()))
        if not targets:
            return
        task = asyncio.create_task(self._broadcast(event_id, targets, message))
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def _broadcast(self, event_id: int, targets: list[WebSocket], message: str) -> None:
        # Параллельно, с таймаутом на каждого: один зависший сокет не тормозит всех.
        results = await asyncio.gather(
            *(asyncio.wait_for(ws.send_text(message), SEND_TIMEOUT_SECONDS) for ws in targets),
            return_exceptions=True,
        )
        for ws, result in zip(targets, results, strict=True):
            if isinstance(result, BaseException):
                self.remove(event_id, ws)


class RedisSubscriber:
    """Одна подписка воркера на каналы Redis; сообщения — синхронным обработчикам."""

    def __init__(self, redis: Redis, handlers: dict[str, Callable[[str], None]]) -> None:
        self._redis = redis
        self._handlers = handlers
        self._task: asyncio.Task | None = None

    async def start(self) -> None:
        # Первая подписка — до приёма запросов, чтобы не пропустить сообщения.
        # Если Redis недоступен, воркер всё равно стартует, подписка — в фоне.
        try:
            pubsub = await self._subscribe()
        except (RedisError, OSError):
            log.warning("pubsub_initial_subscribe_failed", exc_info=True)
            pubsub = None
        self._task = asyncio.create_task(self._run(pubsub), name="redis-subscriber")

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    async def _subscribe(self):
        pubsub = self._redis.pubsub(ignore_subscribe_messages=False)
        await pubsub.subscribe(*self._handlers)
        # Ждём подтверждения по каждому каналу: Pub/Sub не хранит сообщения, и
        # опубликованное до подтверждения пролетело бы мимо.
        confirmed: set[str] = set()
        for _ in range(10):
            message = await pubsub.get_message(timeout=1.0)
            if message is not None and message["type"] == "subscribe":
                confirmed.add(message["channel"])
                if confirmed == set(self._handlers):
                    return pubsub
        await pubsub.aclose()
        raise RedisError("Нет подтверждения подписки")

    async def _run(self, pubsub) -> None:
        # Внешний цикл — переподключение после рестарта Redis.
        while True:
            if pubsub is None:
                pubsub = await self._resubscribe()
            try:
                async for message in pubsub.listen():
                    if message["type"] == "message":
                        self._dispatch(message["channel"], message["data"])
            except asyncio.CancelledError:
                await pubsub.aclose()
                raise
            except (RedisError, OSError):
                log.warning("pubsub_connection_lost", exc_info=True)
            await pubsub.aclose()
            pubsub = None

    async def _resubscribe(self):
        while True:
            await asyncio.sleep(RECONNECT_DELAY_SECONDS)
            try:
                return await self._subscribe()
            except (RedisError, OSError):
                log.warning("pubsub_reconnect_failed")

    def _dispatch(self, channel: str, data: str) -> None:
        try:
            self._handlers[channel](data)
        except Exception:
            log.exception("pubsub_handler_failed", extra={"channel": channel})


def route_by_event(manager: ConnectionManager) -> Callable[[str], None]:
    """Обработчик канала, где в каждом сообщении есть event_id."""

    def handle(raw: str) -> None:
        manager.broadcast_nowait(int(json.loads(raw)["event_id"]), raw)

    return handle
