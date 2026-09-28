"""Каталог: концерты и схема зала — данные, которые читают ВСЕ и одинаково.

Именно поэтому они кэшируются в nginx: при открытии продаж тысячи людей в
секунду запрашивают одну и ту же схему зала, и почти все эти запросы nginx
отдаёт из кэша, не трогая Python и Postgres. Задача API — сказать, насколько
долго ответ можно считать актуальным (Cache-Control).
"""

import json
from typing import Annotated, Any

from fastapi import APIRouter, Path, Request
from fastapi.responses import Response

from app.deps import cacheable, db, error

router = APIRouter(prefix="/api/events")

# Сколько секунд ответ может быть устаревшим без вреда. Каталог меняется редко.
CATALOG_MAX_AGE = 5
# Схема зала меняется постоянно (брони, продажи), но точные изменения придут по
# WebSocket (этап 4) — снимку достаточно быть свежим с точностью до секунды.
SEATMAP_MAX_AGE = 1

EventId = Annotated[int, Path(ge=1)]


def seatmap_version_key(event_id: int) -> str:
    return f"event:{event_id}:seatmap:version"


def json_response(payload: Any, status: int = 200) -> Response:
    # json.dumps напрямую, мимо jsonable_encoder FastAPI: схема зала — это ~2000
    # мест, и обход каждого значения энкодером заметно дороже простой сериализации.
    return Response(
        content=json.dumps(payload, ensure_ascii=False, default=str, separators=(",", ":")),
        status_code=status,
        media_type="application/json",
    )


def event_json(row) -> dict:
    return {
        "id": row["id"],
        "slug": row["slug"],
        "title": row["title"],
        "venue": row["venue"],
        "starts_at": row["starts_at"].isoformat(),
        "sale_opens_at": row["sale_opens_at"].isoformat(),
    }


@router.get("")
async def list_events(request: Request) -> Response:
    async with db(request) as connection:
        rows = await connection.fetch(
            "SELECT id, slug, title, venue, starts_at, sale_opens_at "
            "FROM events ORDER BY starts_at"
        )
    return cacheable(json_response([event_json(row) for row in rows]), CATALOG_MAX_AGE)


@router.get("/{event_id}")
async def get_event(request: Request, event_id: EventId) -> Response:
    async with db(request) as connection:
        event = await connection.fetchrow(
            "SELECT id, slug, title, venue, starts_at, sale_opens_at FROM events WHERE id = $1",
            event_id,
        )
        if event is None:
            return not_found(event_id)
        # Агрегат считает база: в Python пришлось бы тащить все 2000 строк ради
        # четырёх чисел.
        sections = await connection.fetch(
            "SELECT section, min(price_cents) AS price_cents, count(*) AS seats "
            "FROM seats WHERE event_id = $1 GROUP BY section ORDER BY section",
            event_id,
        )
    body = event_json(event) | {
        "sections": [
            {"name": s["section"], "price_cents": s["price_cents"], "seats": s["seats"]}
            for s in sections
        ],
        "total_seats": sum(s["seats"] for s in sections),
    }
    return cacheable(json_response(body), CATALOG_MAX_AGE)


@router.get("/{event_id}/seats")
async def get_seatmap(request: Request, event_id: EventId) -> Response:
    # Версию читаем ДО мест: если между чтениями что-то изменится, снимок окажется
    # новее своей версии, и клиент просто повторно применит уже учтённое изменение
    # (это безопасно). Обратный порядок дал бы снимок СТАРЕЕ версии — и клиент
    # пропустил бы изменение навсегда.
    version = int(await request.app.state.redis.get(seatmap_version_key(event_id)) or 0)
    async with db(request) as connection:
        rows = await connection.fetch(
            "SELECT id, section, row_number, seat_number, price_cents FROM seats "
            "WHERE event_id = $1 ORDER BY section, row_number, seat_number",
            event_id,
        )
    if not rows:
        return not_found(event_id)

    # Компактный формат: место — [id, номер, статус], сгруппировано по секциям и
    # рядам. 2000 мест в виде объектов с именами полей весили бы в разы больше.
    sections: dict[str, dict] = {}
    for row in rows:
        section = sections.setdefault(
            row["section"],
            {"name": row["section"], "price_cents": row["price_cents"], "rows": {}},
        )
        section["rows"].setdefault(row["row_number"], []).append(
            # Статусы held/sold появятся на этапах 4 и 5; пока все места свободны.
            [row["id"], row["seat_number"], "free"]
        )
    body = {
        "event_id": event_id,
        "version": version,
        "sections": [
            section | {"rows": [{"row": n, "seats": seats} for n, seats in section["rows"].items()]}
            for section in sections.values()
        ],
    }
    return cacheable(json_response(body), SEATMAP_MAX_AGE)


def not_found(event_id: int) -> Response:
    # 404 тоже кэшируется: иначе повторные запросы к несуществующему концерту
    # (битая ссылка, которую открывают тысячи) каждый раз шли бы в базу. От
    # перебора РАЗНЫХ id кэш не спасает — это задача rate limit (этап 3).
    response = error(404, f"Концерт {event_id} не найден", "event_not_found")
    return cacheable(response, CATALOG_MAX_AGE)
