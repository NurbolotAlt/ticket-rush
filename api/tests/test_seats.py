"""Бронь мест на уровне Redis: атомарность, лимиты, истечение, порядок версий."""

import asyncio
import json

import pytest

from app.seats import CHANGES_CHANNEL, HoldError, SeatStore, keys
from app.waiting_room import keys as queue_keys
from app.waiting_room import now_ms

TTL = 60_000


async def give_pass(redis, *sessions, event_id=1):
    await redis.zadd(queue_keys(event_id)["active"], {s: now_ms() + TTL for s in sessions})


async def hold(store, sid, seats, ttl=TTL, max_seats=4):
    return await store.hold(1, sid, seats, ttl_ms=ttl, max_seats=max_seats)


async def test_hold_requires_admission_pass(redis):
    with pytest.raises(HoldError) as exc:
        await hold(SeatStore(redis), "nobody", [1])
    assert exc.value.reason == "no_pass"


async def test_all_or_nothing(redis):
    store = SeatStore(redis)
    await give_pass(redis, "alice", "bob")
    await hold(store, "alice", [1, 2])

    with pytest.raises(HoldError) as exc:
        await hold(store, "bob", [2, 3])

    assert exc.value.reason == "taken"
    assert exc.value.seats == [2]
    # Свободное место 3 тоже НЕ занято: пара мест — одна покупка.
    assert await redis.zscore(keys(1)["held"], "3") is None


async def test_concurrent_holds_of_one_seat_have_single_winner(redis):
    store = SeatStore(redis)
    sessions = [f"s{i}" for i in range(50)]
    await give_pass(redis, *sessions)

    results = await asyncio.gather(
        *(hold(store, sid, [7]) for sid in sessions), return_exceptions=True
    )

    winners = [
        sid for sid, r in zip(sessions, results, strict=True) if not isinstance(r, Exception)
    ]
    assert len(winners) == 1
    assert await redis.hget(keys(1)["owner"], "7") == winners[0]


async def test_seat_limit_per_session(redis):
    store = SeatStore(redis)
    await give_pass(redis, "alice")
    await hold(store, "alice", [1, 2, 3])

    with pytest.raises(HoldError) as exc:
        await hold(store, "alice", [4, 5])
    assert exc.value.reason == "too_many"

    await hold(store, "alice", [4])  # ровно до лимита — можно
    seats, _ = await store.my_holds(1, "alice")
    assert seats == [1, 2, 3, 4]


async def test_repeated_hold_of_own_seats_changes_nothing(redis):
    store = SeatStore(redis)
    await give_pass(redis, "alice")
    first_version, _ = await hold(store, "alice", [1])
    second_version, _ = await hold(store, "alice", [1])
    # Статусы не изменились — новой версии и рассылки нет.
    assert first_version == second_version == 1


async def test_only_owner_can_release(redis):
    store = SeatStore(redis)
    await give_pass(redis, "alice", "bob")
    await hold(store, "alice", [1])

    assert not await store.release(1, "bob", 1)
    assert await store.release(1, "alice", 1)
    assert not await store.release(1, "alice", 1)  # второй раз — уже нечего снимать


async def test_expired_hold_is_free_before_and_after_sweep(redis):
    store = SeatStore(redis)
    await give_pass(redis, "alice", "bob")
    await hold(store, "alice", [1], ttl=100)
    await asyncio.sleep(0.2)

    # Ещё не подметено, но по статусам место уже свободно...
    assert 1 not in (await store.snapshot(1))["held"]
    # ...и его можно занять.
    await hold(store, "bob", [1])
    assert await store.sweep_expired(1) == 0  # bob держит его по новой брони


async def test_sweep_frees_and_announces(redis):
    store = SeatStore(redis)
    await give_pass(redis, "alice")
    await hold(store, "alice", [1, 2], ttl=100)
    await asyncio.sleep(0.2)

    assert await store.sweep_expired(1) == 2
    snapshot = await store.snapshot(1)
    assert snapshot["held"] == []
    assert snapshot["version"] == 2  # бронь (1) и освобождение (2)


async def test_sold_seat_cannot_be_held(redis):
    store = SeatStore(redis)
    await give_pass(redis, "alice")
    await redis.sadd(keys(1)["sold"], 5)
    with pytest.raises(HoldError) as exc:
        await hold(store, "alice", [5])
    assert exc.value.reason == "taken"


async def test_changes_are_published_in_version_order(redis):
    # 20 одновременных броней разных мест: подписчик должен получить версии
    # строго 1, 2, ..., 20 — публикация внутри скрипта сохраняет порядок.
    store = SeatStore(redis)
    sessions = [f"s{i}" for i in range(20)]
    await give_pass(redis, *sessions)
    pubsub = redis.pubsub()
    await pubsub.subscribe(CHANGES_CHANNEL)
    await pubsub.get_message(timeout=1)  # подтверждение подписки

    await asyncio.gather(*(hold(store, sid, [i + 1]) for i, sid in enumerate(sessions)))

    versions = []
    while len(versions) < 20:
        message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=2)
        assert message is not None, "не дошли все изменения"
        versions.append(json.loads(message["data"])["version"])
    await pubsub.aclose()
    assert versions == list(range(1, 21))
