"""Покупка: бронь в Redis -> оплата -> билеты в PostgreSQL.

Две системы хранения и внешний платёж — классическая задача "двойной записи":
нельзя атомарно изменить и базу, и Redis, и банк. Поэтому порядок шагов выбран
так, чтобы любой сбой посередине оставлял систему в состоянии, которое можно
исправить, и никогда — в состоянии "деньги взяли, билета нет" без следа:

  1. заказ 'pending' в базе (с ключом идемпотентности) — след есть с самого начала
  2. проверить и продлить брони в Redis
  3. списать деньги (провайдер идемпотентен по id заказа)
  4. ОДНА транзакция в базе: билеты + заказ 'paid'
  5. отметить места проданными в Redis (не удалось — догонит сверка с базой)

База — источник правды. Redis — быстрая копия для броней и схемы зала.
"""

import hashlib
import json
import logging
import secrets
from dataclasses import dataclass

import asyncpg
from fastapi import FastAPI
from redis.exceptions import RedisError

from app.deps import db_for_app
from app.payments import PaymentDeclined

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class OrderResult:
    status_code: int
    body: dict
    replayed: bool = False
    retry_after: int | None = None


# Как неудачный заказ отвечает — и в первый раз, и при повторе с тем же ключом.
FAILURES = {
    "holds_mismatch": (409, "Брони истекли или не совпадают с выбранными местами"),
    "payment_declined": (402, "Платёж отклонён"),
    "seat_already_sold": (409, "Место уже продано"),
    "abandoned": (409, "Оплата не завершилась — попробуйте ещё раз"),
}


def request_hash(event_id: int, seat_ids: list[int]) -> str:
    return hashlib.sha256(json.dumps([event_id, sorted(seat_ids)]).encode()).hexdigest()


async def place_order(
    app: FastAPI, event_id: int, session_id: str, seat_ids: list[int], idempotency_key: str
) -> OrderResult:
    settings = app.state.settings
    seats = app.state.seats
    digest = request_hash(event_id, seat_ids)

    # --- 1. Заказ 'pending' или уже существующий по ключу -------------------
    async with db_for_app(app) as connection:
        # Сумму считает сервер по ценам из базы — клиент её не присылает.
        priced = await connection.fetchrow(
            "SELECT count(*) AS n, coalesce(sum(price_cents), 0) AS amount "
            "FROM seats WHERE event_id = $1 AND id = ANY($2::bigint[])",
            event_id,
            seat_ids,
        )
        if priced["n"] != len(seat_ids):
            return error(422, "Таких мест нет в этом зале", "unknown_seats")
        # ON CONFLICT DO NOTHING: если два запроса с одним ключом пришли разом,
        # второй дождётся, пока первый вставит строку, и получит "конфликт" —
        # заказ создаётся ровно один раз, это гарантирует UNIQUE в базе.
        order_id = await connection.fetchval(
            """
            INSERT INTO orders (event_id, session_id, idempotency_key, request_hash,
                                status, amount_cents, seat_ids)
            VALUES ($1, $2, $3, $4, 'pending', $5, $6)
            ON CONFLICT (session_id, idempotency_key) DO NOTHING
            RETURNING id
            """,
            event_id,
            session_id,
            idempotency_key,
            digest,
            priced["amount"],
            seat_ids,
        )
        if order_id is None:
            return await replay(connection, session_id, idempotency_key, digest)

    ctx = {"order_id": order_id, "event_id": event_id}

    # --- 2. Брони: ровно эти места, живы; продлить на время оплаты -----------
    if not await seats.lock_for_payment(
        event_id, session_id, seat_ids, settings.payment_window_seconds * 1000
    ):
        return await fail(app, order_id, "holds_mismatch")

    # --- 3. Оплата. БЕЗ открытого соединения с базой ------------------------
    # Платёж — сотни миллисекунд ожидания чужой системы. Держи мы в это время
    # соединение из пула (а тем более открытую транзакцию), сотня одновременных
    # оплат заняла бы весь пул, и сайт перестал бы отвечать даже на простое.
    try:
        await app.state.payments.charge(order_id, priced["amount"])
    except PaymentDeclined:
        log.info("payment_declined", extra=ctx)
        return await fail(app, order_id, "payment_declined")

    # --- 4. Билеты и статус заказа — одной транзакцией ----------------------
    # Если база недоступна именно сейчас (деньги уже списаны), исключение уйдёт
    # клиенту как 503, а заказ останется 'pending'. Его подберёт фоновая уборка:
    # через pending_order_timeout пометит неудачным и вернёт деньги.
    tickets = [(order_id, event_id, seat, secrets.token_urlsafe(12)) for seat in seat_ids]
    try:
        async with db_for_app(app) as connection, connection.transaction():
            await connection.executemany(
                "INSERT INTO tickets (order_id, event_id, seat_id, code) VALUES ($1, $2, $3, $4)",
                tickets,
            )
            await connection.execute(
                "UPDATE orders SET status = 'paid', updated_at = now() WHERE id = $1", order_id
            )
    except asyncpg.UniqueViolationError:
        # Сработала последняя линия защиты: место уже продано, хотя Redis считал
        # его нашей бронью (потерял данные, сбой, ошибка в коде). Транзакция
        # откатилась целиком — ни одного билета; деньги возвращаем.
        log.error("double_sale_prevented_by_database", extra=ctx)
        await app.state.payments.refund(order_id)
        await resync_sold(app, event_id, seat_ids)
        return await fail(app, order_id, "seat_already_sold")

    # --- 5. Redis: места проданы. Сбой здесь не страшен ---------------------
    try:
        await seats.mark_sold(event_id, session_id, seat_ids)
    except RedisError:
        # Билеты уже в базе — покупка состоялась. Redis догонит фоновая сверка
        # (лидер раз в 10 с переносит проданные места из базы в Redis).
        log.warning("mark_sold_failed_will_reconcile", extra=ctx, exc_info=True)

    log.info("order_paid", extra=ctx | {"seats": seat_ids, "amount_cents": priced["amount"]})
    return OrderResult(201, await order_body_by_id(app, order_id))


async def replay(
    connection: asyncpg.Connection, session_id: str, idempotency_key: str, digest: str
) -> OrderResult:
    """Запрос с уже использованным ключом: вернуть исход первого запроса."""
    order = await connection.fetchrow(
        "SELECT * FROM orders WHERE session_id = $1 AND idempotency_key = $2",
        session_id,
        idempotency_key,
    )
    if order["request_hash"] != digest:
        return error(
            422, "Этот ключ уже использован для другой покупки", "idempotency_key_reused"
        )
    if order["status"] == "pending":
        # Первый запрос с этим ключом ещё идёт (двойной клик). Повтор через секунду
        # получит его результат.
        return OrderResult(
            409, {"detail": "Оплата уже выполняется", "reason": "order_in_progress"},
            retry_after=1,
        )
    if order["status"] == "failed":
        status, detail = FAILURES[order["failure_reason"]]
        return OrderResult(status, failure_body(order["id"], order["failure_reason"], detail), True)
    return OrderResult(200, await order_body(connection, order), True)


async def fail(app: FastAPI, order_id: int, reason: str) -> OrderResult:
    async with db_for_app(app) as connection:
        await connection.execute(
            "UPDATE orders SET status = 'failed', failure_reason = $2, updated_at = now() "
            "WHERE id = $1",
            order_id,
            reason,
        )
    status, detail = FAILURES[reason]
    return OrderResult(status, failure_body(order_id, reason, detail))


async def resync_sold(app: FastAPI, event_id: int, seat_ids: list[int]) -> None:
    """Перенести из базы в Redis, какие из этих мест на самом деле проданы."""
    async with db_for_app(app) as connection:
        rows = await connection.fetch(
            "SELECT seat_id FROM tickets WHERE event_id = $1 AND seat_id = ANY($2::bigint[])",
            event_id,
            seat_ids,
        )
    try:
        await app.state.seats.restore_sold(event_id, [row["seat_id"] for row in rows])
    except RedisError:
        pass  # догонит фоновая сверка


async def order_body_by_id(app: FastAPI, order_id: int) -> dict:
    async with db_for_app(app) as connection:
        order = await connection.fetchrow("SELECT * FROM orders WHERE id = $1", order_id)
        return await order_body(connection, order)


async def order_body(connection: asyncpg.Connection, order) -> dict:
    tickets = await connection.fetch(
        "SELECT seat_id, code FROM tickets WHERE order_id = $1 ORDER BY seat_id", order["id"]
    )
    return {
        "order_id": order["id"],
        "event_id": order["event_id"],
        "status": order["status"],
        "amount_cents": order["amount_cents"],
        "seat_ids": sorted(order["seat_ids"]),
        "tickets": [{"seat_id": t["seat_id"], "code": t["code"]} for t in tickets],
    }


def failure_body(order_id: int, reason: str, detail: str) -> dict:
    return {"order_id": order_id, "status": "failed", "reason": reason, "detail": detail}


def error(status: int, detail: str, reason: str) -> OrderResult:
    return OrderResult(status, {"detail": detail, "reason": reason})
