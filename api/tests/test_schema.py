"""Миграции и ограничения схемы — на настоящем PostgreSQL."""

from datetime import UTC, datetime, timedelta

import asyncpg
import pytest
from alembic import command
from conftest import alembic_config

NOW = datetime.now(UTC)


async def insert_event(db, slug="e1", opens=NOW, starts=NOW + timedelta(days=1)) -> int:
    return await db.fetchval(
        "INSERT INTO events (slug, title, venue, starts_at, sale_opens_at) "
        "VALUES ($1, 't', 'v', $2, $3) RETURNING id",
        slug,
        starts,
        opens,
    )


def test_migrations_are_reversible(migrated):
    # downgrade -> upgrade на живой базе: миграцию можно откатить при неудачном
    # релизе, и повторное применение не падает.
    config = alembic_config(migrated)
    command.downgrade(config, "base")
    command.upgrade(config, "head")


async def test_same_seat_cannot_exist_twice(db):
    event_id = await insert_event(db)
    insert = (
        "INSERT INTO seats (event_id, section, row_number, seat_number, price_cents) "
        "VALUES ($1, 'A', 1, 1, 100)"
    )
    await db.execute(insert, event_id)
    with pytest.raises(asyncpg.UniqueViolationError):
        await db.execute(insert, event_id)


async def test_sale_must_open_before_concert(db):
    with pytest.raises(asyncpg.CheckViolationError):
        await insert_event(db, opens=NOW + timedelta(days=2), starts=NOW + timedelta(days=1))


async def test_negative_price_rejected(db):
    event_id = await insert_event(db)
    with pytest.raises(asyncpg.CheckViolationError):
        await db.execute(
            "INSERT INTO seats (event_id, section, row_number, seat_number, price_cents) "
            "VALUES ($1, 'A', 1, 1, -1)",
            event_id,
        )


async def test_deleting_event_deletes_its_seats(db):
    event_id = await insert_event(db)
    await db.execute(
        "INSERT INTO seats (event_id, section, row_number, seat_number, price_cents) "
        "VALUES ($1, 'A', 1, 1, 100)",
        event_id,
    )
    await db.execute("DELETE FROM events WHERE id = $1", event_id)
    assert await db.fetchval("SELECT count(*) FROM seats") == 0
