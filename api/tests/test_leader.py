"""Выбор лидера: ровно один впускатель на кластер и переход лидерства при сбое."""

import asyncio

from conftest import running_app

from app.config import SeedSettings
from app.leader import LeaderLease
from app.seed import seed


async def test_only_one_leader(redis):
    a = LeaderLease(redis, "leader:x", "a", ttl_ms=5000)
    b = LeaderLease(redis, "leader:x", "b", ttl_ms=5000)

    assert await a.acquire()
    assert not await b.acquire()
    assert await a.renew()
    assert not await b.renew()  # продлить можно только свою аренду


async def test_lease_expires_when_leader_stops_renewing(redis):
    a = LeaderLease(redis, "leader:x", "a", ttl_ms=200)
    b = LeaderLease(redis, "leader:x", "b", ttl_ms=200)
    await a.acquire()

    await asyncio.sleep(0.3)  # лидер "завис" и не продлевал аренду

    assert await b.acquire()
    assert not await a.renew()  # проснувшийся бывший лидер узнаёт, что его сменили
    assert not await a.release()  # и не может удалить чужую аренду


async def test_admitter_failover_between_workers(settings, db, redis):
    # Два "воркера" с кандидатами во впускатели.
    await seed(db, SeedSettings(seed_sale_opens_in_minutes=-1))
    async with running_app(settings) as first, running_app(settings) as second:
        a, b = first.app.state.admitter, second.app.state.admitter

        await a.step()
        await b.step()
        assert (a.is_leader, b.is_leader) == (True, False)

        await a.stop()  # воркер с лидером останавливается и отдаёт лидерство
        await b.step()
        assert b.is_leader
