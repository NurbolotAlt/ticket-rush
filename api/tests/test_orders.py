"""Покупка: идемпотентность, деньги, и база как последняя защита от двойной продажи."""

import asyncio
import uuid

from conftest import running_app

from app.config import SeedSettings
from app.payments import LEDGER
from app.seats import keys as seat_keys
from app.seed import seed
from app.waiting_room import keys as queue_keys


async def ready_to_buy(client, db, seat_count=2):
    """Концерт с открытыми продажами, клиент впущен и держит бронь."""
    event_id, _ = await seed(db, SeedSettings(seed_sale_opens_in_minutes=-1))
    await client.post(f"/api/events/{event_id}/queue")
    await client.app.state.admitter.step()
    seatmap = (await client.get(f"/api/events/{event_id}/seats")).json()
    seats = [s[0] for s in seatmap["sections"][0]["rows"][0]["seats"][:seat_count]]
    held = await client.post(f"/api/events/{event_id}/holds", json={"seat_ids": seats})
    assert held.status_code == 200
    return event_id, seats


async def buy(client, event_id, seats, key=None):
    return await client.post(
        f"/api/events/{event_id}/orders",
        json={"seat_ids": seats},
        headers={"Idempotency-Key": key or str(uuid.uuid4())},
    )


async def test_purchase_happy_path(settings, db, redis):
    async with running_app(settings) as client:
        event_id, seats = await ready_to_buy(client, db)

        response = await buy(client, event_id, seats)

        assert response.status_code == 201
        order = response.json()
        assert order["status"] == "paid"
        assert [t["seat_id"] for t in order["tickets"]] == seats
        assert len({t["code"] for t in order["tickets"]}) == 2
        # Места проданы — и в схеме зала, и в Redis; брони сняты.
        seatmap = (await client.get(f"/api/events/{event_id}/seats")).json()
        statuses = {s[0]: s[2] for s in seatmap["sections"][0]["rows"][0]["seats"]}
        assert [statuses[s] for s in seats] == ["sold", "sold"]
        assert (await client.get(f"/api/events/{event_id}/holds/me")).json()["seat_ids"] == []
        # Пропуск освобождён — место в "зале ожидания" достанется следующему.
        assert await redis.zcard(queue_keys(event_id)["active"]) == 0
        assert await redis.hget(LEDGER, order["order_id"]) == str(order["amount_cents"])


async def test_replay_returns_same_order_and_charges_once(settings, db, redis):
    async with running_app(settings) as client:
        event_id, seats = await ready_to_buy(client, db)
        key = str(uuid.uuid4())

        first = await buy(client, event_id, seats, key)
        second = await buy(client, event_id, seats, key)

        assert first.status_code == 201
        assert second.status_code == 200
        assert second.headers["idempotent-replayed"] == "true"
        assert second.json()["order_id"] == first.json()["order_id"]
        assert await redis.hlen(LEDGER) == 1
        assert await db.fetchval("SELECT count(*) FROM tickets") == 2


async def test_double_click_creates_one_order_and_one_charge(settings, db, redis):
    async with running_app(settings) as client:
        event_id, seats = await ready_to_buy(client, db)
        key = str(uuid.uuid4())

        responses = await asyncio.gather(*(buy(client, event_id, seats, key) for _ in range(5)))

        codes = sorted(r.status_code for r in responses)
        assert codes.count(201) == 1
        # Остальные: либо повтор готового результата, либо "оплата уже идёт".
        assert set(codes) <= {200, 201, 409}
        assert await db.fetchval("SELECT count(*) FROM orders") == 1
        assert await db.fetchval("SELECT count(*) FROM tickets") == 2
        assert await redis.hlen(LEDGER) == 1


async def test_same_key_for_different_seats_is_rejected(settings, db, redis):
    async with running_app(settings) as client:
        event_id, seats = await ready_to_buy(client, db)
        key = str(uuid.uuid4())
        await buy(client, event_id, seats, key)

        reused = await buy(client, event_id, seats[:1], key)
        assert reused.status_code == 422
        assert reused.json()["reason"] == "idempotency_key_reused"


async def test_idempotency_key_is_required(settings, db, redis):
    async with running_app(settings) as client:
        event_id, seats = await ready_to_buy(client, db)
        response = await client.post(
            f"/api/events/{event_id}/orders", json={"seat_ids": seats}
        )
        assert response.status_code == 400


async def test_cannot_buy_seats_not_held(settings, db, redis):
    async with running_app(settings) as client:
        event_id, seats = await ready_to_buy(client, db, seat_count=1)
        other = seats[0] + 1  # соседнее место без брони

        response = await buy(client, event_id, [other])

        assert response.status_code == 409
        assert response.json()["reason"] == "holds_mismatch"
        assert await redis.hlen(LEDGER) == 0  # денег не взяли


async def test_declined_payment_keeps_holds_for_retry(settings, db, redis):
    settings = settings.model_copy(update={"fake_payment_failure_rate": 1.0})
    async with running_app(settings) as client:
        event_id, seats = await ready_to_buy(client, db)

        response = await buy(client, event_id, seats)

        assert response.status_code == 402
        assert await db.fetchval("SELECT count(*) FROM tickets") == 0
        # Бронь осталась — можно повторить оплату другой картой (новым ключом).
        assert (await client.get(f"/api/events/{event_id}/holds/me")).json()["seat_ids"] == seats


async def test_database_prevents_double_sale_when_redis_is_wrong(settings, db, redis):
    # Самый важный тест: Redis "ошибся" и считает место бронью покупателя, хотя в
    # базе оно уже продано (например, Redis перезапустился и потерял данные).
    async with running_app(settings) as client:
        event_id, seats = await ready_to_buy(client, db, seat_count=1)
        other_order = await db.fetchval(
            "INSERT INTO orders (event_id, session_id, idempotency_key, request_hash, status, "
            "amount_cents, seat_ids) VALUES ($1, 'someone-else', 'k', 'h', 'paid', 0, $2) "
            "RETURNING id",
            event_id,
            seats,
        )
        await db.execute(
            "INSERT INTO tickets (order_id, event_id, seat_id, code) VALUES ($1, $2, $3, 'x')",
            other_order,
            event_id,
            seats[0],
        )

        response = await buy(client, event_id, seats)

        assert response.status_code == 409
        assert response.json()["reason"] == "seat_already_sold"
        # Билет не выдан, деньги возвращены, Redis исправлен по базе.
        assert await db.fetchval("SELECT count(*) FROM tickets") == 1
        assert await redis.hlen(LEDGER) == 0
        assert await redis.sismember(seat_keys(event_id)["sold"], seats[0])


async def test_order_is_visible_only_to_its_owner(settings, db, redis):
    async with running_app(settings) as buyer:
        event_id, seats = await ready_to_buy(buyer, db)
        order_id = (await buy(buyer, event_id, seats)).json()["order_id"]
        assert (await buyer.get(f"/api/orders/{order_id}")).status_code == 200

        buyer.cookies.clear()  # теперь это другой посетитель
        assert (await buyer.get(f"/api/orders/{order_id}")).status_code == 404


async def test_reconcile_restores_sold_seats_lost_by_redis(settings, db, redis):
    async with running_app(settings) as client:
        event_id, seats = await ready_to_buy(client, db)
        await buy(client, event_id, seats)
        await redis.delete(seat_keys(event_id)["sold"])  # Redis "потерял" данные

        restored = await client.app.state.admitter.reconcile_sold(event_id)

        assert restored == 2
        assert (await client.app.state.seats.snapshot(event_id))["sold"] == sorted(seats)


async def test_abandoned_pending_order_is_refunded(settings, db, redis):
    # Процесс упал после списания, но до записи билетов: заказ завис в pending.
    async with running_app(settings) as client:
        event_id, _ = await seed(db, SeedSettings(seed_sale_opens_in_minutes=-1))
        order_id = await db.fetchval(
            "INSERT INTO orders (event_id, session_id, idempotency_key, request_hash, status, "
            "amount_cents, seat_ids, created_at) VALUES "
            "($1, 's', 'k', 'h', 'pending', 500, '{}', now() - interval '10 minutes') RETURNING id",
            event_id,
        )
        await client.app.state.payments.charge(order_id, 500)

        abandoned = await client.app.state.admitter.abandon_stale_orders()

        assert abandoned == [order_id]
        assert not await client.app.state.payments.charged(order_id)
        assert await db.fetchval("SELECT failure_reason FROM orders WHERE id = $1", order_id) == (
            "abandoned"
        )
