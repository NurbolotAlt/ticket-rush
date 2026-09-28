"""Структурированные JSON-логи с request-id.

Каждая строка — JSON с фиксированными полями (timestamp, level, event,
instance_hostname, request_id, ...). request_id генерирует nginx и передаёт в
заголовке X-Request-ID; он же пишется в access-лог nginx. Одним grep по нему
видна вся история запроса: nginx -> какая реплика -> какой воркер -> что делал.

Использование: log.info("seat_held", extra={"event_id": 1}) — сообщение и есть
event, всё из extra становится отдельными полями JSON.
"""

import json
import logging
import socket
import sys
from contextvars import ContextVar
from datetime import UTC, datetime

HOSTNAME = socket.gethostname()

# ContextVar, а не глобальная переменная: в одном воркере одновременно идут сотни
# запросов (asyncio), и у каждой корутины должен быть СВОЙ request_id. ContextVar
# хранит значение отдельно для каждой задачи asyncio.
request_id_var: ContextVar[str | None] = ContextVar("request_id", default=None)

_STANDARD_ATTRS = set(logging.makeLogRecord({}).__dict__) | {"message", "asctime", "taskName"}


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        entry = {
            "timestamp": datetime.fromtimestamp(record.created, tz=UTC).isoformat(),
            "level": record.levelname,
            "event": record.getMessage(),
            "logger": record.name,
            "instance_hostname": HOSTNAME,
        }
        request_id = request_id_var.get()
        if request_id is not None:
            entry["request_id"] = request_id
        for key, value in record.__dict__.items():
            if key not in _STANDARD_ATTRS and not key.startswith("_"):
                entry[key] = value
        if record.exc_info:
            entry["exception"] = self.formatException(record.exc_info)
        return json.dumps(entry, ensure_ascii=False, default=str)


def setup_logging(level: str = "INFO") -> None:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(level)
    # Access-лог пишет nginx (с request_id и адресом реплики) — у uvicorn он дубль.
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
    for name in ("uvicorn", "uvicorn.error"):
        lg = logging.getLogger(name)
        lg.handlers[:] = []
        lg.propagate = True
