"""Наполнение базы: один концерт и зал. Запуск: python -m app.seed

Идемпотентно: повторный запуск ничего не дублирует (ON CONFLICT DO NOTHING), так
что его безопасно выполнять при каждом старте стека.
"""

import asyncio
import logging
from datetime import UTC, datetime, timedelta

import asyncpg

from app.config import SeedSettings
from app.logging_config import setup_logging

log = logging.getLogger(__name__)

# Чем ближе к сцене, тем дороже. Цены в центах.
SECTION_PRICES = {"A": 12_000, "B": 9_000, "C": 6_500, "D": 4_500}
DEFAULT_PRICE = 4_000


async def seed(connection: asyncpg.Connection, settings: SeedSettings) -> tuple[int, int]:
    """Возвращает (event_id, сколько мест в зале)."""
    now = datetime.now(UTC)
    # Весь seed в одной транзакции: либо концерт вместе со всеми местами, либо
    # ничего. Упади процесс посередине — не останется концерта с половиной зала.
    async with connection.transaction():
        event_id = await connection.fetchval(
            """
            INSERT INTO events (slug, title, venue, starts_at, sale_opens_at)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (slug) DO UPDATE SET slug = EXCLUDED.slug
            RETURNING id
            """,
            settings.seed_event_slug,
            "Rush Live 2026",
            "Arena Hall",
            now + timedelta(days=30),
            now + timedelta(minutes=settings.seed_sale_opens_in_minutes),
        )
        # "DO UPDATE SET slug = slug" вместо DO NOTHING — чтобы RETURNING вернул id
        # и для уже существующей строки (с DO NOTHING он вернул бы пустоту).

        rows = [
            (event_id, section, row, seat, SECTION_PRICES.get(section, DEFAULT_PRICE))
            for section in settings.seed_sections.split(",")
            for row in range(1, settings.seed_rows + 1)
            for seat in range(1, settings.seed_seats_per_row + 1)
        ]
        # executemany с готовым запросом: один раз разбирается план, дальше в базу
        # уходят только параметры — тысячи строк за доли секунды.
        await connection.executemany(
            """
            INSERT INTO seats (event_id, section, row_number, seat_number, price_cents)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (event_id, section, row_number, seat_number) DO NOTHING
            """,
            rows,
        )
        total = await connection.fetchval(
            "SELECT count(*) FROM seats WHERE event_id = $1", event_id
        )
    return event_id, total


async def main() -> None:
    settings = SeedSettings()
    setup_logging()
    connection = await asyncpg.connect(settings.database_url)
    try:
        event_id, total = await seed(connection, settings)
        log.info("seed_done", extra={"event_id": event_id, "seats": total})
    finally:
        await connection.close()


if __name__ == "__main__":
    asyncio.run(main())
