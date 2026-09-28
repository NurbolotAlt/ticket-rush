from app.seed import seed


async def test_seed_creates_full_hall(db, settings):
    event_id, total = await seed(db, settings)

    sections = len(settings.seed_sections.split(","))
    assert total == sections * settings.seed_rows * settings.seed_seats_per_row == 2000
    assert await db.fetchval("SELECT count(*) FROM events") == 1
    # Ближние секции дороже дальних.
    prices = dict(
        await db.fetch(
            "SELECT section, max(price_cents) FROM seats WHERE event_id = $1 GROUP BY section",
            event_id,
        )
    )
    assert prices["A"] > prices["D"]


async def test_seed_is_idempotent(db, settings):
    first_id, _ = await seed(db, settings)
    second_id, total = await seed(db, settings)

    assert first_id == second_id
    assert total == 2000
    assert await db.fetchval("SELECT count(*) FROM events") == 1
