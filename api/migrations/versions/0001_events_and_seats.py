"""Концерты и места в зале.

Revision ID: 0001
Revises:
"""

from alembic import op

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
    CREATE TABLE events (
        -- IDENTITY вместо serial: стандарт SQL, и значение нельзя случайно
        -- перезаписать вставкой (GENERATED ALWAYS).
        id              bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        -- Человекочитаемый ключ для URL и для идемпотентного наполнения (seed).
        slug            text        NOT NULL UNIQUE,
        title           text        NOT NULL,
        venue           text        NOT NULL,
        -- timestamptz, а не timestamp: время хранится в UTC и однозначно для
        -- серверов и клиентов в разных часовых поясах.
        starts_at       timestamptz NOT NULL,
        sale_opens_at   timestamptz NOT NULL,
        created_at      timestamptz NOT NULL DEFAULT now(),
        CHECK (sale_opens_at < starts_at)
    )
    """)
    op.execute("""
    CREATE TABLE seats (
        id              bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        event_id        bigint  NOT NULL REFERENCES events (id) ON DELETE CASCADE,
        section         text    NOT NULL,
        row_number      integer NOT NULL CHECK (row_number > 0),
        seat_number     integer NOT NULL CHECK (seat_number > 0),
        -- Деньги — в целых копейках/центах, никогда не float: 0.1 + 0.2 != 0.3.
        price_cents     integer NOT NULL CHECK (price_cents >= 0),
        -- Одно физическое место — одна строка. Этот уникальный индекс заодно
        -- обслуживает частый запрос "все места концерта" (event_id — первая
        -- колонка индекса), отдельный индекс по event_id не нужен.
        UNIQUE (event_id, section, row_number, seat_number)
    )
    """)


def downgrade() -> None:
    op.execute("DROP TABLE seats")
    op.execute("DROP TABLE events")
