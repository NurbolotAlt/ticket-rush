"""Очередь ожидания на уровне Redis: номера, впуск, присутствие, пропуска."""

import asyncio

from app.waiting_room import WaitingRoom, keys, now_ms

LEADER = "leader:test"


async def admit(room: WaitingRoom, redis, *, leader_id="me", **overrides):
    await redis.set(LEADER, "me")
    params = dict(capacity=100, max_admit=10, pass_ttl_ms=60_000, presence_ms=60_000)
    params |= overrides
    return await room.admit(1, leader_key=LEADER, leader_id=leader_id, **params)


async def test_concurrent_joins_get_unique_sequential_numbers(redis):
    room = WaitingRoom(redis)
    numbers = await asyncio.gather(*(room.join(1, f"s{i}") for i in range(300)))
    # Ни дублей, ни пропусков — даже при 300 одновременных входах.
    assert sorted(numbers) == list(range(1, 301))


async def test_rejoin_keeps_place(redis):
    room = WaitingRoom(redis)
    first = await room.join(1, "alice")
    await room.join(1, "bob")
    assert await room.join(1, "alice") == first


async def test_admission_respects_rate_and_capacity(redis):
    room = WaitingRoom(redis)
    for i in range(30):
        await room.join(1, f"s{i}")

    first = await admit(room, redis, max_admit=10, capacity=25)
    second = await admit(room, redis, max_admit=10, capacity=25)
    third = await admit(room, redis, max_admit=10, capacity=25)
    fourth = await admit(room, redis, max_admit=10, capacity=25)

    assert [r.admitted for r in (first, second, third, fourth)] == [10, 10, 5, 0]
    assert fourth.active == 25
    assert fourth.admitted_upto == 25


async def test_non_leader_cannot_admit(redis):
    # Fencing: даже если "бывший лидер" вызовет впуск, скрипт сам проверит аренду.
    room = WaitingRoom(redis)
    await room.join(1, "alice")
    result = await admit(room, redis, leader_id="stale-leader")

    assert not result.is_leader
    assert await redis.get(keys(1)["upto"]) is None
    assert await redis.zcard(keys(1)["active"]) == 0


async def test_absent_users_are_skipped(redis):
    room = WaitingRoom(redis)
    for sid in ("alice", "bob", "carol"):
        await room.join(1, sid)
    # bob закрыл вкладку две минуты назад.
    await redis.zadd(keys(1)["seen"], {"bob": now_ms() - 120_000})

    result = await admit(room, redis, max_admit=10)

    assert (result.admitted, result.skipped) == (2, 1)
    assert await redis.zscore(keys(1)["active"], "bob") is None


async def test_expired_pass_frees_capacity(redis):
    room = WaitingRoom(redis)
    await room.join(1, "alice")
    await room.join(1, "bob")

    assert (await admit(room, redis, capacity=1, pass_ttl_ms=100)).admitted == 1
    assert (await admit(room, redis, capacity=1, pass_ttl_ms=100)).admitted == 0
    await asyncio.sleep(0.2)
    assert (await admit(room, redis, capacity=1, pass_ttl_ms=100)).admitted == 1


async def test_status_lifecycle(redis):
    room = WaitingRoom(redis)
    await room.join(1, "alice")
    await room.join(1, "bob")

    bob = await room.status(1, "bob")
    assert (bob["state"], bob["seq"], bob["position"]) == ("waiting", 2, 2)

    await admit(room, redis, max_admit=1, pass_ttl_ms=100)
    assert (await room.status(1, "alice"))["state"] == "admitted"
    assert (await room.status(1, "bob"))["position"] == 1

    await asyncio.sleep(0.2)
    assert (await room.status(1, "alice"))["state"] == "passed"
    # Вернувшийся после истечения пропуска встаёт в КОНЕЦ, а не на старое место.
    assert await room.join(1, "alice") == 3


async def test_unknown_session_status(redis):
    assert (await WaitingRoom(redis).status(1, "nobody"))["state"] == "not_in_queue"


async def test_pass_of_user_who_left_is_revoked(redis):
    # Впущенный ушёл (не нашёл мест, закрыл вкладку) и молчит дольше presence —
    # его пропуск отзывается, и место покупателя достаётся следующему в очереди.
    room = WaitingRoom(redis)
    for sid in ("alice", "bob"):
        await room.join(1, sid)
    await admit(room, redis, capacity=1)
    assert await redis.zscore(keys(1)["active"], "alice") is not None

    await redis.zadd(keys(1)["seen"], {"alice": now_ms() - 120_000})
    result = await admit(room, redis, capacity=1)

    assert await redis.zscore(keys(1)["active"], "alice") is None
    assert result.admitted == 1  # bob
