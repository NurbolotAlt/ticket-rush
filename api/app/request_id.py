"""ASGI-middleware: request_id из заголовка X-Request-ID (его ставит nginx).

Чистый ASGI, а не BaseHTTPMiddleware из Starlette: тот гонит каждый ответ через
дополнительную задачу и канал в памяти — лишняя работа на горячем пути под
нагрузкой. Здесь — только установка ContextVar и заголовок в ответе.
"""

import re
import uuid

from starlette.types import ASGIApp, Message, Receive, Scope, Send

from app.logging_config import request_id_var

HEADER = b"x-request-id"
# Принимаем только "похожее на id": заголовок мог прийти и не от nginx (тесты,
# прямой доступ в dev), а без проверки в логи попадали бы произвольные строки.
_VALID = re.compile(r"^[A-Za-z0-9-]{8,64}$")


class RequestIdMiddleware:
    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] not in ("http", "websocket"):
            await self.app(scope, receive, send)
            return

        incoming = dict(scope["headers"]).get(HEADER, b"").decode("latin-1")
        request_id = incoming if _VALID.match(incoming) else uuid.uuid4().hex
        token = request_id_var.set(request_id)

        if scope["type"] == "websocket":
            # Для WebSocket — только id в логах. Оборачивать send незачем: заголовок
            # ставится лишь в HTTP-ответ, а обёртка на КАЖДОЕ сообщение соединения
            # стоила заметных процентов CPU при массовой рассылке (профиль py-spy).
            try:
                await self.app(scope, receive, send)
            finally:
                request_id_var.reset(token)
            return

        async def send_with_header(message: Message) -> None:
            # Возвращаем id клиенту: в отчёте об ошибке пользователь может его
            # назвать, и мы найдём этот запрос в логах.
            if message["type"] == "http.response.start":
                message.setdefault("headers", []).append((HEADER, request_id.encode()))
            await send(message)

        try:
            await self.app(scope, receive, send_with_header)
        finally:
            request_id_var.reset(token)
