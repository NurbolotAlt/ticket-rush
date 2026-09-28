"""Анонимные сессии в подписанной cookie.

Сессия — случайный id, который браузер хранит в cookie. На сервере сессии не
хранятся: подпись HMAC доказывает, что id выдали мы, а не придумал клиент.
Поэтому любая реплика проверяет cookie без похода в Redis.

Зачем подпись: id сессии — ключ к месту в очереди. Без подписи можно было бы
подставить чужой id (перебором или подсмотрев) и занять чужой номер.
"""

import base64
import hashlib
import hmac
import secrets

from fastapi import Response
from starlette.requests import HTTPConnection

from app.config import Settings

COOKIE_NAME = "sid"
COOKIE_MAX_AGE = 24 * 3600


def _sign(session_id: str, secret: str) -> str:
    digest = hmac.new(secret.encode(), session_id.encode(), hashlib.sha256).digest()
    return base64.urlsafe_b64encode(digest[:18]).decode().rstrip("=")


def encode(session_id: str, secret: str) -> str:
    return f"{session_id}.{_sign(session_id, secret)}"


def decode(cookie: str | None, secret: str) -> str | None:
    """id сессии, если подпись верна, иначе None."""
    if not cookie or "." not in cookie:
        return None
    session_id, signature = cookie.rsplit(".", 1)
    # compare_digest — сравнение за постоянное время: обычный == выходит на первом
    # несовпавшем символе, и по времени ответа подпись можно подбирать по байту.
    if hmac.compare_digest(signature, _sign(session_id, secret)):
        return session_id
    return None


def current_session(connection: HTTPConnection) -> str | None:
    # HTTPConnection — общий предок Request и WebSocket: cookie сессии браузер
    # присылает и при открытии WebSocket, так что проверка одна для обоих.
    settings: Settings = connection.app.state.settings
    return decode(connection.cookies.get(COOKIE_NAME), settings.session_secret)


def new_session_id() -> str:
    return secrets.token_urlsafe(16)


def set_session_cookie(response: Response, settings: Settings, session_id: str) -> None:
    response.set_cookie(
        COOKIE_NAME,
        encode(session_id, settings.session_secret),
        max_age=COOKIE_MAX_AGE,
        # HttpOnly: JavaScript страницы cookie не видит — XSS не украдёт сессию.
        httponly=True,
        secure=settings.session_cookie_secure,
        # Lax: cookie не уходит с POST-запросами с чужих сайтов (защита от CSRF),
        # но переход по ссылке на наш сайт сессию сохраняет.
        samesite="lax",
        path="/",
    )
