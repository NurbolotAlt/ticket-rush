"""HTTP и WebSocket очереди ожидания.

Все ответы здесь ПЕРСОНАЛЬНЫЕ (номер, пропуск конкретной сессии) и помечаются
Cache-Control: no-store. Это вторая линия защиты: путь /api/events/{id}/queue
начинается с кэшируемого префикса /api/events, и без запрета кэширования nginx
мог бы отдать "ваш номер 57" всем. (Первая линия — nginx кэширует только точные
пути каталога.)
"""

import asyncio
import logging
import time
from datetime import UTC, datetime
from typing import Annotated

from fastapi import APIRouter, Path, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse, Response

from app.deps import client_ip, db, error
from app.rate_limit import check_rate_limit
from app.sessions import current_session, new_session_id, set_session_cookie

log = logging.getLogger(__name__)

router = APIRouter()

EventId = Annotated[int, Path(ge=1)]

# Как часто браузер подтверждает, что вкладка открыта (сообщение по WebSocket).
HEARTBEAT_SECONDS = 15


def personal(response: Response) -> Response:
    response.headers["Cache-Control"] = "no-store"
    return response


class EventScheduleCache:
    """Время открытия продаж — в памяти воркера на несколько секунд.

    При открытии продаж тысячи людей в секунду жмут "встать в очередь", и каждому
    нужно знать, открыты ли продажи. Спрашивать базу на каждый клик — самый
    простой способ её положить; расписание концерта меняется раз в никогда.
    """

    TTL_SECONDS = 5

    def __init__(self) -> None:
        self._items: dict[int, tuple[float, tuple[datetime, datetime] | None]] = {}
        # Лок на концерт: при промахе в базу идёт один запрос, а не сотня
        # одновременных (тот же приём против stampede, в миниатюре).
        self._locks: dict[int, asyncio.Lock] = {}

    async def get(self, request: Request, event_id: int) -> tuple[datetime, datetime] | None:
        cached = self._items.get(event_id)
        if cached and time.monotonic() - cached[0] < self.TTL_SECONDS:
            return cached[1]
        async with self._locks.setdefault(event_id, asyncio.Lock()):
            cached = self._items.get(event_id)
            if cached and time.monotonic() - cached[0] < self.TTL_SECONDS:
                return cached[1]
            async with db(request) as connection:
                row = await connection.fetchrow(
                    "SELECT sale_opens_at, starts_at FROM events WHERE id = $1", event_id
                )
            value = (row["sale_opens_at"], row["starts_at"]) if row else None
            self._items[event_id] = (time.monotonic(), value)
            return value


@router.post("/api/events/{event_id}/queue")
async def join_queue(request: Request, event_id: EventId) -> Response:
    settings = request.app.state.settings
    session_id = current_session(request)
    is_new_session = session_id is None
    if is_new_session:
        session_id = new_session_id()

    schedule = await request.app.state.schedule.get(request, event_id)
    if schedule is None:
        return personal(error(404, f"Концерт {event_id} не найден", "event_not_found"))
    opens_at, starts_at = schedule
    now = datetime.now(UTC)
    if now < opens_at:
        # Встать в очередь заранее нельзя: иначе выигрывал бы тот, кто (или чей
        # бот) пришёл раньше всех. Честнее — предочередь с перемешиванием в
        # момент открытия; это возможное улучшение.
        body = error(409, "Продажи ещё не открыты", "sale_not_open")
        body.headers["Retry-After"] = str(max(1, int((opens_at - now).total_seconds())))
        return personal(body)
    if now >= starts_at:
        return personal(error(409, "Продажи закрыты", "sale_closed"))

    # Два лимита. По сессии — строгий. Но бот, не хранящий cookie, получает новую
    # сессию на КАЖДЫЙ запрос, и лимит по сессии его не остановит — поэтому ещё
    # лимит по IP. Он щедрее: за одним IP бывает офис или мобильный оператор
    # (NAT), и сотни честных людей не должны делить пять попыток на всех.
    redis = request.app.state.redis
    for key, limit_per_minute in (
        (f"ratelimit:join:ip:{client_ip(request)}", settings.join_rate_limit_per_ip_per_minute),
        (f"ratelimit:join:sid:{session_id}", settings.join_rate_limit_per_minute),
    ):
        limit = await check_rate_limit(redis, key, limit_per_minute, 60)
        if not limit.allowed:
            return personal(error(429, "Слишком часто", "rate_limit", limit.retry_after))

    room = request.app.state.room
    seq = await room.join(event_id, session_id)
    log.info("queue_joined", extra={"event_id": event_id, "seq": seq})
    response = personal(JSONResponse(await room.status(event_id, session_id)))
    if is_new_session:
        set_session_cookie(response, settings, session_id)
    return response


@router.get("/api/events/{event_id}/queue/me")
async def queue_status(request: Request, event_id: EventId) -> Response:
    session_id = current_session(request)
    if session_id is None:
        return personal(JSONResponse({"event_id": event_id, "state": "not_in_queue"}))
    status = await request.app.state.room.status(event_id, session_id)
    return personal(JSONResponse(status))


@router.websocket("/ws/events/{event_id}/queue")
async def queue_updates(websocket: WebSocket, event_id: EventId) -> None:
    """Состояние очереди концерта в реальном времени (одно общее для всех).

    Клиент раз в HEARTBEAT_SECONDS присылает "ping" — это признак, что вкладка
    открыта. Кто перестал присылать, того при подходе очереди пропустят.
    """
    manager = websocket.app.state.manager
    room = websocket.app.state.room
    session_id = current_session(websocket)
    await websocket.accept()
    manager.add(event_id, websocket)
    try:
        snapshot = await room.current_state(event_id)
        if snapshot:
            await websocket.send_text(snapshot)
        while True:
            await websocket.receive_text()
            if session_id is not None:
                await room.heartbeat(event_id, session_id)
    except WebSocketDisconnect:
        pass
    finally:
        manager.remove(event_id, websocket)
