"""HTTP броней и единый WebSocket страницы концерта."""

import json
import logging
from typing import Annotated

from fastapi import APIRouter, Path, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse, Response
from pydantic import BaseModel, Field

from app.deps import db, error
from app.queue_routes import personal
from app.seats import HoldError
from app.sessions import current_session

log = logging.getLogger(__name__)

router = APIRouter()

EventId = Annotated[int, Path(ge=1)]
SeatId = Annotated[int, Path(ge=1)]


class HoldRequest(BaseModel):
    # Верхняя граница на уровне схемы: тело с тысячей id отсекается до Redis.
    # Точный лимит "мест в одни руки" (с учётом уже забронированных) — в скрипте.
    seat_ids: list[Annotated[int, Field(ge=1)]] = Field(min_length=1, max_length=10)


HOLD_ERRORS = {
    "no_pass": (403, "Сначала дождитесь своей очереди"),
    "taken": (409, "Часть мест уже занята — выберите другие"),
    "too_many": (409, "Слишком много мест в одни руки"),
}


@router.post("/api/events/{event_id}/holds")
async def hold_seats(request: Request, event_id: EventId, body: HoldRequest) -> Response:
    session_id = current_session(request)
    if session_id is None:
        return personal(error(403, "Сначала встаньте в очередь", "no_pass"))
    settings = request.app.state.settings

    seat_ids = sorted(set(body.seat_ids))
    known = await request.app.state.seat_catalog.seat_ids(lambda: db(request), event_id)
    if not known:
        return personal(error(404, f"Концерт {event_id} не найден", "event_not_found"))
    if unknown := [seat for seat in seat_ids if seat not in known]:
        # Места другого концерта или выдуманные id — до Redis не доходим.
        response = error(422, "Таких мест нет в этом зале", "unknown_seats")
        return personal(response_with_seats(response, unknown))

    try:
        version, expires_ms = await request.app.state.seats.hold(
            event_id,
            session_id,
            seat_ids,
            ttl_ms=settings.hold_ttl_seconds * 1000,
            max_seats=settings.max_seats_per_session,
        )
    except HoldError as exc:
        status, text = HOLD_ERRORS[exc.reason]
        log.info("hold_rejected", extra={"event_id": event_id, "reason": exc.reason})
        return personal(response_with_seats(error(status, text, exc.reason), exc.seats))

    held, _ = await request.app.state.seats.my_holds(event_id, session_id)
    log.info("seats_held", extra={"event_id": event_id, "seats": seat_ids, "version": version})
    return personal(
        JSONResponse({"seat_ids": held, "expires_at": expires_ms // 1000, "version": version})
    )


@router.delete("/api/events/{event_id}/holds/{seat_id}")
async def release_seat(request: Request, event_id: EventId, seat_id: SeatId) -> Response:
    session_id = current_session(request)
    if session_id is None or not await request.app.state.seats.release(
        event_id, session_id, seat_id
    ):
        # Одинаковый ответ для "не ваша" и "нет такой брони": не подсказываем,
        # какие места держат другие.
        return personal(error(404, "У вас нет брони на это место", "hold_not_found"))
    return personal(Response(status_code=204))


@router.get("/api/events/{event_id}/holds/me")
async def my_holds(request: Request, event_id: EventId) -> Response:
    session_id = current_session(request)
    if session_id is None:
        return personal(JSONResponse({"seat_ids": [], "expires_at": None}))
    seats, expires_ms = await request.app.state.seats.my_holds(event_id, session_id)
    return personal(
        JSONResponse({"seat_ids": seats, "expires_at": expires_ms // 1000 if expires_ms else None})
    )


def response_with_seats(response: JSONResponse, seats: list[int]) -> JSONResponse:
    """Добавить в тело ошибки список проблемных мест — клиент подсветит их."""
    body = json.loads(response.body)
    return JSONResponse(body | {"seat_ids": seats}, status_code=response.status_code)


@router.websocket("/ws/events/{event_id}")
async def event_updates(websocket: WebSocket, event_id: EventId) -> None:
    """Всё живое на странице концерта — по одному соединению.

    Сразу после подключения: состояние очереди и ТОЧНЫЙ снимок статусов мест с
    версией (снимок из кэша nginx может отставать на секунду). Дальше —
    изменения очереди и мест. Клиент сверяет версии: пришла версия через одну —
    значит, что-то потерялось, и он переподключается за свежим снимком.

    Клиент раз в 15 с шлёт "ping": это признак, что вкладка открыта, — без него
    при подходе очереди его пропустят.
    """
    manager = websocket.app.state.manager
    room = websocket.app.state.room
    session_id = current_session(websocket)
    await websocket.accept()
    # Сначала подписываемся на изменения, ПОТОМ читаем снимок: изменение, случившееся
    # между этими шагами, придёт отдельным сообщением (и будет отброшено клиентом,
    # если уже вошло в снимок). В обратном порядке его можно было бы потерять.
    client = manager.add(event_id, websocket, session_id)
    try:
        queue_state = await room.current_state(event_id)
        if queue_state:
            manager.send(event_id, client, queue_state)
        snapshot = await websocket.app.state.seats.snapshot(event_id)
        manager.send(event_id, client, json.dumps(snapshot, separators=(",", ":")))
        while True:
            await websocket.receive_text()
            if session_id is not None:
                await room.heartbeat(event_id, session_id)
    except WebSocketDisconnect:
        pass
    finally:
        manager.remove(event_id, client)
