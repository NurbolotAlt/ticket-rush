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

# Закрытие соединения с отставшим клиентом не должно висеть вечно.
CLOSE_TIMEOUT_SECONDS = 2.0
# Сколько сообщений может скопиться у клиента. Больше — он безнадёжно отстал:
# отключаем, он переподключится и получит свежий снимок вместо сотни изменений.
OUTBOX_LIMIT = 200
RECONNECT_DELAY_SECONDS = 1.0


class Client:
    """WebSocket-клиент со СВОЕЙ очередью исходящих и одним отправителем.

    Порядок важен: изменения схемы зала пронумерованы версиями, и клиент,
    получивший 58 раньше 57, решит, что пропустил сообщение. Если рассылать
    каждое сообщение отдельной задачей, две задачи могут отправить одному клиенту
    в обратном порядке. Один отправитель на клиента сохраняет порядок, а очередь
    изолирует медленного: остальные его не ждут.
    """

    def __init__(self, websocket: WebSocket, event_id: int, session_id: str | None) -> None:
        self.websocket = websocket
        self.event_id = event_id
        # По сессии находим вкладки покупателя для ЛИЧНЫХ сообщений ("ваши билеты
        # готовы") — они не должны уходить всем зрителям концерта.
        self.session_id = session_id
        self.outbox: asyncio.Queue[str] = asyncio.Queue(maxsize=OUTBOX_LIMIT)
        self.sender: asyncio.Task | None = None


class ConnectionManager:
    """WebSocket-клиенты ОДНОГО воркера, сгруппированные по концерту."""

    def __init__(self) -> None:
        self._by_event: dict[int, set[Client]] = defaultdict(set)
        self._by_session: dict[str, set[Client]] = defaultdict(set)

    def count(self, event_id: int | None = None) -> int:
        if event_id is not None:
            return len(self._by_event.get(event_id, ()))
        return sum(len(clients) for clients in self._by_event.values())

    def add(self, event_id: int, websocket: WebSocket, session_id: str | None = None) -> Client:
        client = Client(websocket, event_id, session_id)
        client.sender = asyncio.create_task(self._send_loop(event_id, client))
        self._by_event[event_id].add(client)
        if session_id is not None:
            self._by_session[session_id].add(client)
        return client

    def remove(self, event_id: int, client: Client) -> None:
        clients = self._by_event.get(event_id)
        if clients is not None:
            clients.discard(client)
            if not clients:
                del self._by_event[event_id]
        if client.session_id is not None:
            mine = self._by_session.get(client.session_id)
            if mine is not None:
                mine.discard(client)
                if not mine:
                    del self._by_session[client.session_id]
        if client.sender is not None and client.sender is not asyncio.current_task():
            client.sender.cancel()

    def send(self, event_id: int, client: Client, message: str) -> None:
        try:
            client.outbox.put_nowait(message)
        except asyncio.QueueFull:
            # Медленного (или зависшего) клиента ловит переполнение ЕГО очереди, а не
            # таймаут на каждую отправку: таймер на каждое сообщение каждому клиенту
            # стоил ~8% CPU воркера под нагрузкой (профиль py-spy). Остальных он не
            # тормозит и так — у каждого клиента свой отправитель.
            log.warning("ws_client_too_slow", extra={"event_id": event_id})
            self.remove(event_id, client)
            # Закрываем в фоне: нас вызвали синхронно из обработчика подписки.
            asyncio.create_task(self._close(client))

    def broadcast_nowait(self, event_id: int, message: str) -> None:
        """Для обработчика подписки: разложить по очередям клиентов и вернуться."""
        for client in list(self._by_event.get(event_id, ())):
            self.send(event_id, client, message)

    def send_to_session(self, session_id: str, message: str) -> None:
        """Личное сообщение — во все вкладки этой сессии (на этом воркере)."""
        for client in list(self._by_session.get(session_id, ())):
            self.send(client.event_id, client, message)

    async def _send_loop(self, event_id: int, client: Client) -> None:
        try:
            while True:
                message = await client.outbox.get()
                await client.websocket.send_text(message)
        except asyncio.CancelledError:
            raise
        except Exception:
            # Соединение оборвалось — убираем и закрываем: иначе браузер мог бы
            # считать себя подключённым, а изменения ему больше не шли бы. Он
            # переподключится и получит свежий снимок.
            self.remove(event_id, client)
            await self._close(client)

    @staticmethod
    async def _close(client: Client) -> None:
        try:
            # 1013 "Try Again Later": клиенту стоит переподключиться.
            await asyncio.wait_for(client.websocket.close(code=1013), CLOSE_TIMEOUT_SECONDS)
        except Exception:
            pass


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


class SeatChangeBatcher:
    """Склеивает изменения мест за короткое окно в одно сообщение на клиента.

    Профиль под нагрузкой: больше всего CPU воркеров уходило на рассылку — каждая
    бронь/покупка/освобождение отдельным сообщением каждому из сотен зрителей.
    За 150 мс происходят десятки изменений; одно сообщение с диапазоном версий
    вместо десятков — во столько же раз меньше отправок. Цена — схема зала
    обновляется с задержкой до окна, для зрителя незаметной.

    Склеиваются только НЕПРЕРЫВНЫЕ версии: если между ними дыра (сообщение
    потерялось), отправляем раздельно — клиент увидит пропуск и возьмёт снимок.
    """

    def __init__(self, manager: ConnectionManager, window_seconds: float) -> None:
        self._manager = manager
        self._window = window_seconds
        self._pending: dict[int, list[dict]] = {}

    def handle(self, raw: str) -> None:
        message = json.loads(raw)
        event_id = int(message["event_id"])
        if event_id not in self._pending:
            self._pending[event_id] = []
            asyncio.get_running_loop().call_later(self._window, self._flush, event_id)
        self._pending[event_id].append(message)

    def _flush(self, event_id: int) -> None:
        messages = sorted(self._pending.pop(event_id, []), key=lambda m: m["version"])
        for run in contiguous_runs(messages):
            # Порядок важен: у места побеждает ПОСЛЕДНИЙ статус в окне
            # (забронировали и сразу купили -> "sold").
            merged: dict[int, str] = {}
            for message in run:
                for seat, status in message["changes"]:
                    merged.pop(seat, None)
                    merged[seat] = status
            self._manager.broadcast_nowait(
                event_id,
                json.dumps(
                    {
                        "type": "seats",
                        "event_id": event_id,
                        "from_version": run[0]["version"],
                        "version": run[-1]["version"],
                        "changes": [[seat, status] for seat, status in merged.items()],
                    },
                    separators=(",", ":"),
                ),
            )


def contiguous_runs(messages: list[dict]) -> list[list[dict]]:
    runs: list[list[dict]] = []
    for message in messages:
        if runs and message["version"] == runs[-1][-1]["version"] + 1:
            runs[-1].append(message)
        else:
            runs.append([message])
    return runs


def route_to_session(manager: ConnectionManager) -> Callable[[str], None]:
    """Обработчик канала личных сообщений: {"session_id": ..., "payload": {...}}.

    Сообщение получают ВСЕ воркеры (вкладка покупателя может быть подключена к
    любому), но доставляет только тот, у кого есть сокеты этой сессии.
    """

    def handle(raw: str) -> None:
        message = json.loads(raw)
        manager.send_to_session(message["session_id"], json.dumps(message["payload"]))

    return handle


def route_by_event(manager: ConnectionManager) -> Callable[[str], None]:
    """Обработчик канала, где в каждом сообщении есть event_id."""

    def handle(raw: str) -> None:
        manager.broadcast_nowait(int(json.loads(raw)["event_id"]), raw)

    return handle
