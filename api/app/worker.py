"""Фоновый воркер: генерирует PDF-билеты. Запуск: python -m app.worker

Отдельный контейнер, а не задача внутри API: тяжёлая CPU-работа не должна
отнимать процессор у обработки запросов во время распродажи, и масштабируется
она отдельно (docker compose --scale worker=N).

Внутри — asyncio-цикл чтения очереди + ProcessPoolExecutor для рендеринга
(CPU-работа, GIL — см. compute-service). Задач в работе одновременно не больше,
чем процессов в пуле: лишние остаются в Redis, а не копятся в памяти воркера.

Гарантия доставки — "хотя бы один раз": задача может прийти повторно (воркер
упал после рендеринга, но до XACK). Поэтому обработка идемпотентна.
"""

import asyncio
import logging
import multiprocessing
import os
import signal
import socket
from concurrent.futures import ProcessPoolExecutor

import asyncpg
from redis.asyncio import BlockingConnectionPool, Redis
from redis.exceptions import ResponseError

from app.config import WorkerSettings
from app.logging_config import setup_logging
from app.redis_client import create_redis_client
from app.ticket_jobs import DEAD_LETTERS, GROUP, STREAM, notify_order
from app.ticket_pdf import render_tickets_pdf

log = logging.getLogger(__name__)

# Записи воркера (счётчик попыток, готовый PDF) коммитятся БЕЗ ожидания сброса
# журнала (WAL) на диск. Под нагрузкой база простаивала, а время уходило именно
# на COMMIT — ожидание диска (выборка pg_stat_activity), плюс каждый PDF целиком
# пишется в WAL. Эти записи восстановимы: при падении ПОСТГРЕСА (не приложения)
# теряются лишь последние доли секунды, и сверка лидера перегенерирует PDF.
# Целостность базы асинхронный коммит не нарушает. Заказы и билеты — деньги —
# по-прежнему коммитятся синхронно, это настройка только этих транзакций.
ASYNC_COMMIT = "SET LOCAL synchronous_commit TO OFF"


class TicketWorker:
    def __init__(
        self,
        settings: WorkerSettings,
        redis: Redis,
        db: asyncpg.Pool,
        executor: ProcessPoolExecutor,
        name: str | None = None,
    ) -> None:
        self._settings = settings
        self._redis = redis
        self._db = db
        self._executor = executor
        self.name = name or f"{socket.gethostname()}:{os.getpid()}"
        self._slots = asyncio.Semaphore(settings.worker_concurrency)
        self._in_flight: set[asyncio.Task] = set()
        self._stopping = asyncio.Event()

    async def ensure_group(self) -> None:
        try:
            # id "0": группа увидит и задачи, добавленные до её создания.
            # MKSTREAM: создать поток, если задач ещё не было.
            await self._redis.xgroup_create(STREAM, GROUP, id="0", mkstream=True)
        except ResponseError as exc:
            if "BUSYGROUP" not in str(exc):  # группа уже есть — это нормально
                raise

    def stop(self) -> None:
        self._stopping.set()

    async def run(self) -> None:
        await self.ensure_group()
        log.info("worker_started", extra={"consumer": self.name})
        claim_every = max(self._settings.worker_claim_idle_ms / 2000, 1)
        last_claim = 0.0
        while not self._stopping.is_set():
            loop_time = asyncio.get_running_loop().time()
            if loop_time - last_claim > claim_every:
                last_claim = loop_time
                for message_id, fields in await self.claim_stale():
                    await self._start(message_id, fields)
            for message_id, fields in await self.read(block_ms=2000):
                await self._start(message_id, fields)
        # Graceful shutdown: новых задач не берём, начатые доделываем. Неначатые
        # (не прочитанные) останутся в Redis для других воркеров.
        if self._in_flight:
            await asyncio.gather(*self._in_flight, return_exceptions=True)
        log.info("worker_stopped", extra={"consumer": self.name})

    async def read(self, block_ms: int) -> list[tuple[str, dict]]:
        # Читаем не больше, чем есть свободных процессов: задачи, которые некому
        # обработать, пусть ждут в Redis (там их может взять другой воркер).
        free = max(self._settings.worker_concurrency - len(self._in_flight), 1)
        response = await self._redis.xreadgroup(
            GROUP, self.name, {STREAM: ">"}, count=free, block=block_ms
        )
        return [entry for _, entries in response for entry in entries] if response else []

    async def claim_stale(self) -> list[tuple[str, dict]]:
        """Забрать задачи, которые другой воркер взял и не подтвердил (упал/завис)."""
        _, claimed, _ = await self._redis.xautoclaim(
            STREAM,
            GROUP,
            self.name,
            min_idle_time=self._settings.worker_claim_idle_ms,
            start_id="0-0",
            count=10,
        )
        if claimed:
            log.warning("jobs_claimed_from_dead_consumer", extra={"count": len(claimed)})
        return claimed

    async def _start(self, message_id: str, fields: dict) -> None:
        await self._slots.acquire()
        task = asyncio.create_task(self._guarded(message_id, fields))
        self._in_flight.add(task)
        task.add_done_callback(self._in_flight.discard)

    async def _guarded(self, message_id: str, fields: dict) -> None:
        try:
            await self.handle(message_id, fields)
        except Exception:
            # Не подтверждаем: задача останется в pending и вернётся через
            # XAUTOCLAIM. Число попыток считает база (pdf_attempts).
            log.exception("ticket_job_failed", extra={"message_id": message_id})
        finally:
            self._slots.release()

    async def handle(self, message_id: str, fields: dict) -> None:
        order_id = int(fields["order_id"])
        ctx = {"order_id": order_id, "message_id": message_id}
        async with self._db.acquire(timeout=5) as connection:
            # Счётчик попыток увеличиваем ДО работы и коммитим ОТДЕЛЬНОЙ транзакцией:
            # если загрузка или рендеринг упадёт, попытка всё равно засчитается.
            # (В одной транзакции с работой откат стирал бы и счётчик — тогда
            # "отравленная" задача крутилась бы вечно; это ловит тест.)
            async with connection.transaction():
                await connection.execute(ASYNC_COMMIT)
                order = await connection.fetchrow(
                    """
                    UPDATE orders SET pdf_attempts = pdf_attempts + 1
                    WHERE id = $1
                    RETURNING id, session_id, event_id, status, pdf_status, pdf_attempts
                    """,
                    order_id,
                )
            if order is None or order["status"] != "paid" or order["pdf_status"] == "ready":
                # Нечего делать или уже сделано (повторная доставка) — подтверждаем.
                await self._redis.xack(STREAM, GROUP, message_id)
                return
            if order["pdf_attempts"] > self._settings.worker_max_attempts:
                await self._give_up(connection, order, message_id)
                return
            data = await self._load(connection, order_id)

        # Рендеринг — в процессе пула, без открытого соединения с базой.
        loop = asyncio.get_running_loop()
        pdf = await loop.run_in_executor(
            self._executor, render_tickets_pdf, data[0], data[1], self._settings.pdf_font_path
        )

        async with self._db.acquire(timeout=5) as connection, connection.transaction():
            await connection.execute(ASYNC_COMMIT)
            # ON CONFLICT DO NOTHING: если два воркера всё же сделали одну задачу
            # (один завис, задачу забрал другой, первый ожил) — останется один PDF.
            await connection.execute(
                "INSERT INTO ticket_pdfs (order_id, pdf) VALUES ($1, $2) "
                "ON CONFLICT (order_id) DO NOTHING",
                order_id,
                pdf,
            )
            await connection.execute(
                "UPDATE orders SET pdf_status = 'ready', updated_at = now() WHERE id = $1",
                order_id,
            )
        # Подтверждаем ПОСЛЕ записи в базу: упади мы между ними — задача вернётся,
        # и повтор просто увидит pdf_status = 'ready'.
        await self._redis.xack(STREAM, GROUP, message_id)
        await notify_order(
            self._redis,
            order["session_id"],
            {"type": "order", "order_id": order_id, "pdf_status": "ready"},
        )
        log.info("ticket_pdf_ready", extra=ctx | {"bytes": len(pdf)})

    async def _give_up(self, connection: asyncpg.Connection, order, message_id: str) -> None:
        """"Отравленная" задача: падает всегда. Убираем из очереди в отдельный поток
        для разбора человеком — иначе она крутилась бы вечно и отнимала воркер."""
        await connection.execute(
            "UPDATE orders SET pdf_status = 'failed', updated_at = now() WHERE id = $1",
            order["id"],
        )
        await self._redis.xadd(DEAD_LETTERS, {"order_id": order["id"], "message_id": message_id})
        await self._redis.xack(STREAM, GROUP, message_id)
        await notify_order(
            self._redis,
            order["session_id"],
            {"type": "order", "order_id": order["id"], "pdf_status": "failed"},
        )
        log.error("ticket_job_dead_lettered", extra={"order_id": order["id"]})

    @staticmethod
    async def _load(connection: asyncpg.Connection, order_id: int) -> tuple[dict, list[dict]]:
        rows = await connection.fetch(
            """
            SELECT o.id AS order_id, e.title, e.venue, e.starts_at,
                   t.code, s.section, s.row_number, s.seat_number, s.price_cents
            FROM orders o
            JOIN events e ON e.id = o.event_id
            JOIN tickets t ON t.order_id = o.id
            JOIN seats s ON s.id = t.seat_id
            WHERE o.id = $1
            ORDER BY s.section, s.row_number, s.seat_number
            """,
            order_id,
        )
        first = rows[0]
        order = {
            "order_id": first["order_id"],
            "title": first["title"],
            "venue": first["venue"],
            "starts_at": first["starts_at"].strftime("%d.%m.%Y %H:%M UTC"),
        }
        tickets = [
            {
                "code": row["code"],
                "section": row["section"],
                "row": row["row_number"],
                "seat": row["seat_number"],
                "price": f"${row['price_cents'] / 100:.2f}",
            }
            for row in rows
        ]
        return order, tickets


async def main() -> None:
    settings = WorkerSettings()
    setup_logging()
    # Пул процессов: spawn — чистый интерпретатор без копий сокетов и локов
    # родителя (подробно — в compute-service).
    executor = ProcessPoolExecutor(
        max_workers=settings.worker_concurrency,
        mp_context=multiprocessing.get_context("spawn"),
    )
    redis = create_redis_client(
        BlockingConnectionPool.from_url(
            settings.redis_url, max_connections=10, timeout=5, decode_responses=True
        )
    )
    # Маленький пул: воркер делает 2-3 коротких запроса на задачу.
    db = await asyncpg.create_pool(settings.database_url, min_size=1, max_size=3)
    worker = TicketWorker(settings, redis, db, executor)

    # SIGTERM от Docker (docker compose stop, деплой) — мягкая остановка.
    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGTERM, worker.stop)
    loop.add_signal_handler(signal.SIGINT, worker.stop)
    try:
        await worker.run()
    finally:
        await db.close()
        await redis.aclose()
        executor.shutdown(wait=True)


if __name__ == "__main__":
    asyncio.run(main())
