"""Очередь задач генерации билетов на Redis Streams.

Почему Stream, а не Pub/Sub (который уже есть в проекте): Pub/Sub — "кто слушает
сейчас, тот и получил", без хранения. Stream хранит задачу, пока её не подтвердят
(XACK), а consumer group раздаёт каждую задачу ровно одному воркеру из группы.
Упал воркер посреди работы — задача остаётся в списке "выдана, не подтверждена"
(pending entries list), и её забирает другой.
"""

import json

from redis.asyncio import Redis

STREAM = "jobs:tickets"
GROUP = "ticket-pdf"
DEAD_LETTERS = "jobs:tickets:dead"
# Персональные уведомления о заказах (в отличие от общих каналов концерта).
ORDERS_CHANNEL = "orders:updates"
# Поток не растёт бесконечно: подтверждённые задачи из него не удаляются сами.
# ~ (приблизительно) позволяет Redis обрезать целыми блоками — это дешевле точной.
STREAM_MAX_LEN = 100_000


async def enqueue_ticket_pdf(redis: Redis, order_id: int) -> None:
    await redis.xadd(STREAM, {"order_id": order_id}, maxlen=STREAM_MAX_LEN, approximate=True)


async def notify_order(redis: Redis, session_id: str, payload: dict) -> None:
    # session_id — адрес доставки (по нему воркеры находят WebSocket-вкладки этой
    # сессии); клиенту он уходит без него.
    await redis.publish(ORDERS_CHANNEL, json.dumps({"session_id": session_id, "payload": payload}))
