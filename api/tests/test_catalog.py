"""Каталог и схема зала: содержимое ответов и кэш-заголовки для nginx."""

from conftest import running_app

from app.catalog import seatmap_version_key
from app.config import SeedSettings
from app.seed import seed


async def test_list_events(settings, db, redis):
    event_id, _ = await seed(db, SeedSettings())
    async with running_app(settings) as client:
        response = await client.get("/api/events")
        assert response.status_code == 200
        assert [e["id"] for e in response.json()] == [event_id]
        assert response.headers["cache-control"] == "public, max-age=5"


async def test_event_details_with_sections(settings, db, redis):
    event_id, _ = await seed(db, SeedSettings())
    async with running_app(settings) as client:
        body = (await client.get(f"/api/events/{event_id}")).json()
        assert body["total_seats"] == 2000
        assert [s["name"] for s in body["sections"]] == ["A", "B", "C", "D"]
        assert all(s["seats"] == 500 for s in body["sections"])


async def test_seatmap_has_every_seat_once(settings, db, redis):
    event_id, _ = await seed(db, SeedSettings())
    async with running_app(settings) as client:
        response = await client.get(f"/api/events/{event_id}/seats")
        body = response.json()

        seat_ids = [
            seat[0]
            for section in body["sections"]
            for row in section["rows"]
            for seat in row["seats"]
        ]
        assert len(seat_ids) == len(set(seat_ids)) == 2000
        assert response.headers["cache-control"] == "public, max-age=1"
        first_row = body["sections"][0]["rows"][0]
        assert first_row["row"] == 1
        assert [seat[1] for seat in first_row["seats"]] == list(range(1, 26))


async def test_seatmap_carries_version_from_redis(settings, db, redis):
    # Версия схемы — задел для этапа 4: клиент сверяет её с версиями изменений,
    # пришедших по WebSocket.
    event_id, _ = await seed(db, SeedSettings())
    await redis.set(seatmap_version_key(event_id), 42)
    async with running_app(settings) as client:
        assert (await client.get(f"/api/events/{event_id}/seats")).json()["version"] == 42


async def test_unknown_event_is_404_and_cacheable(settings, db, redis):
    async with running_app(settings) as client:
        for path in ("/api/events/999", "/api/events/999/seats"):
            response = await client.get(path)
            assert response.status_code == 404
            assert response.json()["reason"] == "event_not_found"
            assert "max-age" in response.headers["cache-control"]


async def test_invalid_event_id_rejected(settings, db, redis):
    async with running_app(settings) as client:
        assert (await client.get("/api/events/0")).status_code == 422
        assert (await client.get("/api/events/abc")).status_code == 422
