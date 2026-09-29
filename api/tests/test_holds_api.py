"""Бронь через HTTP и живые изменения схемы зала по WebSocket."""

import redis as sync_redis
from conftest import running_app
from fastapi.testclient import TestClient

from app.config import SeedSettings
from app.main import create_app
from app.seed import seed


async def admitted_client(client, event_id):
    await client.post(f"/api/events/{event_id}/queue")
    await client.app.state.admitter.step()


async def first_seats(client, event_id, count):
    seatmap = (await client.get(f"/api/events/{event_id}/seats")).json()
    return [seat[0] for seat in seatmap["sections"][0]["rows"][0]["seats"][:count]]


async def open_sale(db):
    event_id, _ = await seed(db, SeedSettings(seed_sale_opens_in_minutes=-1))
    return event_id


async def test_hold_and_see_it_in_seatmap(settings, db, redis):
    event_id = await open_sale(db)
    async with running_app(settings) as client:
        await admitted_client(client, event_id)
        seats = await first_seats(client, event_id, 2)

        response = await client.post(f"/api/events/{event_id}/holds", json={"seat_ids": seats})
        assert response.status_code == 200
        assert response.json()["seat_ids"] == seats
        assert response.headers["cache-control"] == "no-store"

        seatmap = (await client.get(f"/api/events/{event_id}/seats")).json()
        statuses = {s[0]: s[2] for s in seatmap["sections"][0]["rows"][0]["seats"]}
        assert [statuses[s] for s in seats] == ["held", "held"]
        assert seatmap["version"] == 1


async def test_hold_without_admission_is_forbidden(settings, db, redis):
    event_id = await open_sale(db)
    async with running_app(settings) as client:
        await client.post(f"/api/events/{event_id}/queue")  # в очереди, но не впущен
        seats = await first_seats(client, event_id, 1)
        response = await client.post(f"/api/events/{event_id}/holds", json={"seat_ids": seats})
        assert response.status_code == 403
        assert response.json()["reason"] == "no_pass"


async def test_unknown_seats_rejected_before_redis(settings, db, redis):
    event_id = await open_sale(db)
    async with running_app(settings) as client:
        await admitted_client(client, event_id)
        response = await client.post(
            f"/api/events/{event_id}/holds", json={"seat_ids": [999_999]}
        )
        assert response.status_code == 422
        assert response.json()["seat_ids"] == [999_999]


async def test_too_many_ids_rejected_by_schema(settings, db, redis):
    event_id = await open_sale(db)
    async with running_app(settings) as client:
        response = await client.post(
            f"/api/events/{event_id}/holds", json={"seat_ids": list(range(1, 50))}
        )
        assert response.status_code == 422


async def test_release_own_hold(settings, db, redis):
    event_id = await open_sale(db)
    async with running_app(settings) as client:
        await admitted_client(client, event_id)
        [seat] = await first_seats(client, event_id, 1)
        await client.post(f"/api/events/{event_id}/holds", json={"seat_ids": [seat]})

        assert (await client.delete(f"/api/events/{event_id}/holds/{seat}")).status_code == 204
        assert (await client.delete(f"/api/events/{event_id}/holds/{seat}")).status_code == 404
        assert (await client.get(f"/api/events/{event_id}/holds/me")).json()["seat_ids"] == []


def test_websocket_snapshot_then_live_changes(settings):
    # Зритель открыл страницу: получает точный снимок статусов, затем — изменения
    # с версиями по порядку. Тест синхронный (TestClient умеет WebSocket), поэтому
    # чистую базу и Redis готовит сам, без асинхронных фикстур.
    sync_redis.Redis.from_url(settings.redis_url).flushdb()
    app = create_app(settings)
    with TestClient(app) as buyer:
        event_id = buyer.portal.call(open_sale_sync, settings.database_url)
        buyer.post(f"/api/events/{event_id}/queue")
        buyer.portal.call(app.state.admitter.step)
        seatmap = buyer.get(f"/api/events/{event_id}/seats").json()
        seat = seatmap["sections"][0]["rows"][0]["seats"][0][0]

        with buyer.websocket_connect(f"/ws/events/{event_id}") as ws:
            snapshot = next_of_type(ws, "seats_snapshot")
            assert snapshot["held"] == []

            buyer.post(f"/api/events/{event_id}/holds", json={"seat_ids": [seat]})
            change = next_of_type(ws, "seats")
            assert change["version"] == snapshot["version"] + 1
            assert change["changes"] == [[seat, "held"]]

            buyer.delete(f"/api/events/{event_id}/holds/{seat}")
            change = next_of_type(ws, "seats")
            assert change["changes"] == [[seat, "free"]]


def next_of_type(ws, message_type: str) -> dict:
    while True:
        message = ws.receive_json()
        if message.get("type") == message_type:
            return message


async def open_sale_sync(database_url: str) -> int:
    import asyncpg

    connection = await asyncpg.connect(database_url)
    try:
        await connection.execute("TRUNCATE events RESTART IDENTITY CASCADE")
        event_id, _ = await seed(connection, SeedSettings(seed_sale_opens_in_minutes=-1))
        return event_id
    finally:
        await connection.close()
