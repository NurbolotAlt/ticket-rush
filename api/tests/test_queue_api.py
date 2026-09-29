"""Очередь через HTTP и WebSocket: сессии, лимиты, кэш-заголовки, впуск."""

import json

import redis as sync_redis
from conftest import running_app
from fastapi.testclient import TestClient

from app.config import SeedSettings
from app.main import create_app
from app.seed import seed
from app.waiting_room import STATE_CHANNEL


async def open_sale(db) -> int:
    event_id, _ = await seed(db, SeedSettings(seed_sale_opens_in_minutes=-1))
    return event_id


async def test_join_issues_signed_session_cookie(settings, db, redis):
    event_id = await open_sale(db)
    async with running_app(settings) as client:
        response = await client.post(f"/api/events/{event_id}/queue")

        assert response.status_code == 200
        assert response.json()["state"] == "waiting"
        assert response.json()["seq"] == 1
        cookie = response.headers["set-cookie"].lower()
        assert "httponly" in cookie and "samesite=lax" in cookie
        # Персональный ответ nginx и браузер кэшировать не должны.
        assert response.headers["cache-control"] == "no-store"

        again = await client.post(f"/api/events/{event_id}/queue")
        assert again.json()["seq"] == 1
        assert "set-cookie" not in again.headers  # сессия уже есть


async def test_forged_cookie_is_not_trusted(settings, db, redis):
    event_id = await open_sale(db)
    async with running_app(settings) as client:
        first = await client.post(f"/api/events/{event_id}/queue")
        session_id = client.cookies["sid"].split(".")[0]
        # Чужой id с поддельной подписью — должен считаться новым посетителем.
        client.cookies.set("sid", f"{session_id}.forged-signature")

        forged = await client.post(f"/api/events/{event_id}/queue")
        assert first.json()["seq"] == 1
        assert forged.json()["seq"] == 2


async def test_cannot_join_before_sale_opens(settings, db, redis):
    event_id, _ = await seed(db, SeedSettings(seed_sale_opens_in_minutes=10))
    async with running_app(settings) as client:
        response = await client.post(f"/api/events/{event_id}/queue")
        assert response.status_code == 409
        assert response.json()["reason"] == "sale_not_open"
        assert int(response.headers["retry-after"]) > 500


async def test_unknown_event(settings, db, redis):
    async with running_app(settings) as client:
        response = await client.post("/api/events/999/queue")
        assert response.status_code == 404


async def test_join_rate_limit_per_session(settings, db, redis):
    event_id = await open_sale(db)
    async with running_app(settings) as client:
        codes = [
            (await client.post(f"/api/events/{event_id}/queue")).status_code for _ in range(6)
        ]
        assert codes == [200] * 5 + [429]


async def test_join_rate_limit_per_ip_stops_cookieless_bots(settings, db, redis):
    # Бот без cookie получает новую сессию на каждый запрос — остановит его
    # только лимит по IP.
    settings = settings.model_copy(update={"join_rate_limit_per_ip_per_minute": 3})
    event_id = await open_sale(db)
    async with running_app(settings) as client:
        codes = []
        for _ in range(4):
            client.cookies.clear()
            response = await client.post(
                f"/api/events/{event_id}/queue", headers={"X-Real-IP": "203.0.113.9"}
            )
            codes.append(response.status_code)
        assert codes == [200, 200, 200, 429]


async def test_admitted_after_admitter_tick(settings, db, redis):
    event_id = await open_sale(db)
    async with running_app(settings) as client:
        await client.post(f"/api/events/{event_id}/queue")
        assert (await client.get(f"/api/events/{event_id}/queue/me")).json()["state"] == "waiting"

        await client.app.state.admitter.step()

        status = (await client.get(f"/api/events/{event_id}/queue/me")).json()
        assert status["state"] == "admitted"
        assert status["pass_expires_at"] > 0


async def test_status_without_session(settings, db, redis):
    async with running_app(settings) as client:
        response = await client.get("/api/events/1/queue/me")
        assert response.json()["state"] == "not_in_queue"
        assert response.headers["cache-control"] == "no-store"


def test_websocket_receives_queue_state(settings, redis_url):
    # Состояние, опубликованное в Redis (любым воркером-лидером), доходит до
    # WebSocket-клиентов этого концерта и не доходит до клиентов другого.
    app = create_app(settings)
    publisher = sync_redis.Redis.from_url(redis_url)
    with TestClient(app) as client, client.websocket_connect("/ws/events/7") as ws:
        # Первым сервер присылает снимок статусов мест.
        assert ws.receive_json()["type"] == "seats_snapshot"
        publisher.publish(STATE_CHANNEL, json.dumps({"type": "queue", "event_id": 8}))
        publisher.publish(
            STATE_CHANNEL, json.dumps({"type": "queue", "event_id": 7, "admitted_upto": 42})
        )
        assert ws.receive_json()["admitted_upto"] == 42
    publisher.close()
