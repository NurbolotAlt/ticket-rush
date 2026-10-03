"""Воркер PDF-билетов: очередь на Redis Streams, сбои, повторы, "отравленные" задачи."""

import asyncio
import json
import multiprocessing
from concurrent.futures import ProcessPoolExecutor

import asyncpg
import pytest
from conftest import running_app
from test_orders import buy, ready_to_buy

from app.config import WorkerSettings
from app.ticket_jobs import DEAD_LETTERS, GROUP, ORDERS_CHANNEL, STREAM, enqueue_ticket_pdf
from app.worker import TicketWorker


@pytest.fixture(scope="session")
def executor():
    # Настоящий пул процессов (spawn) — как в контейнере; один на все тесты, чтобы
    # не платить за старт процессов в каждом.
    with ProcessPoolExecutor(2, mp_context=multiprocessing.get_context("spawn")) as pool:
        yield pool


@pytest.fixture
async def make_worker(settings, redis, executor):
    pools = []

    async def factory(name: str) -> TicketWorker:
        db = await asyncpg.create_pool(settings.database_url, min_size=1, max_size=3)
        pools.append(db)
        worker_settings = WorkerSettings(
            database_url=settings.database_url,
            redis_url=settings.redis_url,
            worker_concurrency=2,
            worker_claim_idle_ms=200,
            worker_max_attempts=2,
        )
        worker = TicketWorker(worker_settings, redis, db, executor, name=name)
        await worker.ensure_group()
        return worker

    yield factory
    for db in pools:
        await db.close()


async def paid_order(client, db) -> int:
    event_id, seats = await ready_to_buy(client, db)
    response = await buy(client, event_id, seats)
    assert response.status_code == 201
    assert response.json()["pdf_status"] == "pending"  # ответ не ждёт рендеринга
    return response.json()["order_id"]


async def pending_count(redis) -> int:
    return (await redis.xpending(STREAM, GROUP))["pending"]


async def test_job_produces_pdf_and_notifies_owner(settings, db, redis, make_worker):
    async with running_app(settings) as client:
        order_id = await paid_order(client, db)
        worker = await make_worker("w1")
        pubsub = redis.pubsub()
        await pubsub.subscribe(ORDERS_CHANNEL)
        await pubsub.get_message(timeout=1)

        [(message_id, fields)] = await worker.read(block_ms=500)
        await worker.handle(message_id, fields)

        pdf = await client.get(f"/api/orders/{order_id}/tickets.pdf")
        assert pdf.status_code == 200
        assert pdf.headers["content-type"] == "application/pdf"
        assert pdf.content.startswith(b"%PDF-")
        assert (await client.get(f"/api/orders/{order_id}")).json()["pdf_url"]
        assert await pending_count(redis) == 0  # задача подтверждена

        note = await pubsub.get_message(ignore_subscribe_messages=True, timeout=2)
        payload = json.loads(note["data"])
        assert payload["payload"] == {"type": "order", "order_id": order_id, "pdf_status": "ready"}
        await pubsub.aclose()


async def test_pdf_is_not_given_before_ready_or_to_others(settings, db, redis):
    async with running_app(settings) as client:
        order_id = await paid_order(client, db)
        pending = await client.get(f"/api/orders/{order_id}/tickets.pdf")
        assert pending.status_code == 202
        assert pending.headers["retry-after"] == "2"

        client.cookies.clear()
        assert (await client.get(f"/api/orders/{order_id}/tickets.pdf")).status_code == 404


async def test_duplicate_delivery_creates_one_pdf(settings, db, redis, make_worker):
    # "Хотя бы один раз": задача может прийти дважды — результат должен быть один.
    async with running_app(settings) as client:
        order_id = await paid_order(client, db)
        await enqueue_ticket_pdf(redis, order_id)  # дубль
        worker = await make_worker("w1")

        messages = await worker.read(block_ms=500)
        assert len(messages) == 2
        for message_id, fields in messages:
            await worker.handle(message_id, fields)

        assert await db.fetchval("SELECT count(*) FROM ticket_pdfs") == 1
        assert await pending_count(redis) == 0


async def test_job_of_crashed_worker_is_picked_up_by_another(settings, db, redis, make_worker):
    async with running_app(settings) as client:
        order_id = await paid_order(client, db)
        crashed, survivor = await make_worker("crashed"), await make_worker("survivor")

        assert len(await crashed.read(block_ms=500)) == 1  # взял задачу и "упал"
        assert await survivor.claim_stale() == []  # ещё рано: вдруг он просто медленный
        await asyncio.sleep(0.3)  # прошло больше worker_claim_idle_ms

        [(message_id, fields)] = await survivor.claim_stale()
        await survivor.handle(message_id, fields)

        status = await db.fetchval("SELECT pdf_status FROM orders WHERE id = $1", order_id)
        assert status == "ready"


async def test_poison_job_goes_to_dead_letters(settings, db, redis, make_worker):
    # Заказ без билетов — рендеринг падает каждый раз. После worker_max_attempts
    # задача уходит в разбор, а не крутится вечно.
    async with running_app(settings) as client:
        order_id = await paid_order(client, db)
        await db.execute("DELETE FROM tickets WHERE order_id = $1", order_id)
        worker = await make_worker("w1")
        [(message_id, fields)] = await worker.read(block_ms=500)

        for _ in range(2):
            with pytest.raises(IndexError):
                await worker.handle(message_id, fields)
        await worker.handle(message_id, fields)  # третья попытка — сдаёмся

        assert await db.fetchval("SELECT pdf_status FROM orders WHERE id = $1", order_id) == (
            "failed"
        )
        assert await redis.xlen(DEAD_LETTERS) == 1
        assert await pending_count(redis) == 0


async def test_paid_orders_without_pdf_are_requeued(settings, db, redis):
    async with running_app(settings) as client:
        order_id = await paid_order(client, db)
        await redis.delete(STREAM)  # постановка задачи "не удалась"
        await db.execute(
            "UPDATE orders SET updated_at = now() - interval '10 minutes' WHERE id = $1",
            order_id,
        )

        requeued = await client.app.state.admitter.requeue_missing_pdfs()

        assert requeued == [order_id]
        assert await redis.xlen(STREAM) == 1
