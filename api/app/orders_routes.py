"""HTTP покупки и заказов."""

import re
from typing import Annotated

from fastapi import APIRouter, Header, Path, Request
from fastapi.responses import JSONResponse, Response
from pydantic import BaseModel, Field

from app.deps import db, error
from app.orders import order_body, place_order
from app.queue_routes import personal
from app.sessions import current_session

router = APIRouter()

EventId = Annotated[int, Path(ge=1)]
OrderId = Annotated[int, Path(ge=1)]
# UUID от браузера (crypto.randomUUID) или любой похожий случайный идентификатор.
_IDEMPOTENCY_KEY = re.compile(r"^[A-Za-z0-9-]{16,64}$")


class OrderRequest(BaseModel):
    # Клиент называет места, которые видит в корзине: сервер сверит их с бронями.
    # Без этого после истечения брони и повторного выбора можно было бы купить не
    # то, что пользователь видит на экране.
    seat_ids: list[Annotated[int, Field(ge=1)]] = Field(min_length=1, max_length=10)


@router.post("/api/events/{event_id}/orders")
async def create_order(
    request: Request,
    event_id: EventId,
    body: OrderRequest,
    idempotency_key: Annotated[str | None, Header()] = None,
) -> Response:
    session_id = current_session(request)
    if session_id is None:
        return personal(error(403, "Сначала встаньте в очередь", "no_pass"))
    # Ключ обязателен: без него повтор после обрыва связи создал бы второй заказ
    # и второе списание. Лучше отказать сразу, чем однажды списать дважды.
    if idempotency_key is None or not _IDEMPOTENCY_KEY.match(idempotency_key):
        return personal(error(400, "Нужен заголовок Idempotency-Key", "idempotency_key_required"))

    result = await place_order(
        request.app, event_id, session_id, sorted(set(body.seat_ids)), idempotency_key
    )
    headers = {"Idempotent-Replayed": "true"} if result.replayed else {}
    if result.retry_after is not None:
        headers["Retry-After"] = str(result.retry_after)
    return personal(JSONResponse(result.body, status_code=result.status_code, headers=headers))


@router.get("/api/orders/{order_id}")
async def get_order(request: Request, order_id: OrderId) -> Response:
    session_id = current_session(request)
    async with db(request) as connection:
        # Заказ виден только своей сессии. Чужой — тот же 404, что и несуществующий:
        # не подтверждаем, что заказ с таким номером вообще есть.
        order = await connection.fetchrow(
            "SELECT * FROM orders WHERE id = $1 AND session_id = $2", order_id, session_id
        )
        if order is None:
            return personal(error(404, "Заказ не найден", "order_not_found"))
        return personal(JSONResponse(await order_body(connection, order)))


@router.get("/api/orders/{order_id}/tickets.pdf")
async def ticket_pdf(request: Request, order_id: OrderId) -> Response:
    session_id = current_session(request)
    async with db(request) as connection:
        row = await connection.fetchrow(
            "SELECT o.pdf_status, p.pdf FROM orders o "
            "LEFT JOIN ticket_pdfs p ON p.order_id = o.id "
            "WHERE o.id = $1 AND o.session_id = $2 AND o.status = 'paid'",
            order_id,
            session_id,
        )
    if row is None:
        return personal(error(404, "Заказ не найден", "order_not_found"))
    if row["pdf"] is None:
        # Ещё рендерится: 202 + Retry-After. Обычно клиент узнаёт о готовности по
        # WebSocket и сюда за неготовым файлом не приходит.
        if row["pdf_status"] == "failed":
            return personal(error(500, "Не удалось подготовить билеты", "pdf_failed"))
        return personal(error(202, "Билеты готовятся", "pdf_pending", 2))
    return personal(
        Response(
            content=row["pdf"],
            media_type="application/pdf",
            headers={"Content-Disposition": f'inline; filename="tickets-{order_id}.pdf"'},
        )
    )
