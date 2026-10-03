"""Заказы и билеты.

Revision ID: 0002
Revises: 0001
"""

from alembic import op

revision = "0002"
down_revision = "0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Нужен для составного внешнего ключа из tickets: билет может ссылаться только
    # на место СВОЕГО концерта. Без него ошибка в коде могла бы продать "место 7"
    # концерта A как билет на концерт B.
    op.execute("ALTER TABLE seats ADD CONSTRAINT seats_event_id_id_key UNIQUE (event_id, id)")

    op.execute("""
    CREATE TABLE orders (
        id               bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        event_id         bigint      NOT NULL REFERENCES events (id),
        session_id       text        NOT NULL,
        -- Ключ идемпотентности от клиента: повтор запроса с тем же ключом вернёт
        -- тот же заказ, а не создаст второй (и не спишет деньги дважды).
        idempotency_key  text        NOT NULL,
        -- Отпечаток тела запроса: тот же ключ с ДРУГИМИ местами — ошибка клиента,
        -- а не повтор, и молча вернуть старый заказ было бы неправильно.
        request_hash     text        NOT NULL,
        status           text        NOT NULL CHECK (status IN ('pending', 'paid', 'failed')),
        failure_reason   text,
        amount_cents     integer     NOT NULL CHECK (amount_cents >= 0),
        seat_ids         bigint[]    NOT NULL,
        created_at       timestamptz NOT NULL DEFAULT now(),
        updated_at       timestamptz NOT NULL DEFAULT now(),
        UNIQUE (session_id, idempotency_key)
    )
    """)
    # Частичный индекс: зависшие заказы ищет фоновая уборка, а их единицы среди
    # тысяч оплаченных. Индекс только по pending — крошечный и быстрый.
    op.execute("CREATE INDEX orders_pending_created_at ON orders (created_at) "
               "WHERE status = 'pending'")

    op.execute("""
    CREATE TABLE tickets (
        id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        order_id    bigint      NOT NULL REFERENCES orders (id),
        event_id    bigint      NOT NULL,
        seat_id     bigint      NOT NULL,
        -- Код билета для QR (этап 6). Случайный, а не id: по id можно перебрать
        -- чужие билеты.
        code        text        NOT NULL UNIQUE,
        created_at  timestamptz NOT NULL DEFAULT now(),
        FOREIGN KEY (event_id, seat_id) REFERENCES seats (event_id, id),
        -- ГЛАВНАЯ гарантия проекта: одно место — один билет. Даже если Redis
        -- потерял данные или в коде брони ошибка, база не даст продать дважды.
        UNIQUE (event_id, seat_id)
    )
    """)
    op.execute("CREATE INDEX tickets_order_id ON tickets (order_id)")


def downgrade() -> None:
    op.execute("DROP TABLE tickets")
    op.execute("DROP TABLE orders")
    op.execute("ALTER TABLE seats DROP CONSTRAINT seats_event_id_id_key")
