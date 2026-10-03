# syntax=docker/dockerfile:1

# Стадия 1: зависимости ставятся в отдельный venv. Кэш pip и всё, что нужно
# только для сборки, в финальный образ не попадает.
FROM python:3.12-slim AS builder

ENV PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# requirements.txt — замок с точными версиями и хэшами (собран pip-compile из
# requirements.in). Отдельным слоем: пересобирается только при его изменении.
COPY requirements.txt .
RUN pip install -r requirements.txt

# Стадия 2: только интерпретатор, готовый venv и код.
FROM python:3.12-slim

# curl — только для HEALTHCHECK. fonts-dejavu-core — шрифт с кириллицей для
# PDF-билетов (воркер использует тот же образ, что и API).
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl fonts-dejavu-core \
    && rm -rf /var/lib/apt/lists/*

# Непривилегированный пользователь: уязвимость в приложении даёт атакующему права
# app, а не root. Код принадлежит root и только читается — подменить его нельзя.
RUN useradd --system --no-create-home --uid 10001 app

COPY --from=builder /opt/venv /opt/venv

WORKDIR /srv
COPY alembic.ini ./
COPY migrations ./migrations
COPY app ./app

ENV PATH="/opt/venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    WEB_CONCURRENCY=2

USER app

EXPOSE 8000

# Без HEALTHCHECK Docker видит только "процесс жив/не жив". С ним — отвечает ли
# приложение и видит ли Redis и Postgres; на это опирается depends_on nginx.
HEALTHCHECK --interval=15s --timeout=3s --start-period=20s --retries=3 \
    CMD curl -fsS http://localhost:8000/api/health || exit 1

# "app.main:create_app()" — gunicorn сам вызывает фабрику приложения.
# Gunicorn — мастер-процесс (порт, перезапуск упавших воркеров, graceful
# shutdown), UvicornWorker — asyncio event loop в каждом воркере.
# Число воркеров — из WEB_CONCURRENCY (gunicorn читает его сам).
# --graceful-timeout 30: на SIGTERM воркеры дорабатывают текущие запросы.
# --keep-alive 75: дольше, чем keepalive_timeout в upstream nginx (60 с), чтобы
#   простаивающее соединение закрывал nginx, а не backend (иначе редкие 502).
# --worker-tmp-dir /dev/shm: heartbeat воркера в RAM, а не на overlay-диске.
# --no-control-socket: управляющий сокет gunicorn (с 25.1) нам не нужен.
CMD ["gunicorn", "app.main:create_app()", \
     "--worker-class", "uvicorn_worker.UvicornWorker", \
     "--bind", "0.0.0.0:8000", \
     "--timeout", "90", \
     "--graceful-timeout", "30", \
     "--keep-alive", "75", \
     "--worker-tmp-dir", "/dev/shm", \
     "--no-control-socket"]
