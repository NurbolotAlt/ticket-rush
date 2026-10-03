"""PDF-билеты, которые генерирует фоновый воркер.

Revision ID: 0003
Revises: 0002
"""

from alembic import op

revision = "0003"
down_revision = "0002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # DEFAULT 'pending': и уже оплаченные заказы, и новые получают статус сразу —
    # сверка лидера найдёт их и поставит в очередь генерации.
    op.execute("""
    ALTER TABLE orders
        ADD COLUMN pdf_status text NOT NULL DEFAULT 'pending'
            CHECK (pdf_status IN ('pending', 'ready', 'failed')),
        -- Сколько раз воркер брался за этот заказ: "отравленная" задача, которая
        -- падает всегда, после нескольких попыток уходит в разбор, а не крутится вечно.
        ADD COLUMN pdf_attempts integer NOT NULL DEFAULT 0
    """)
    # Частичный индекс для сверки "оплачено, а PDF всё нет": таких заказов единицы.
    op.execute("CREATE INDEX orders_pdf_pending ON orders (updated_at) "
               "WHERE status = 'paid' AND pdf_status = 'pending'")
    op.execute("""
    CREATE TABLE ticket_pdfs (
        -- Один PDF на заказ (все билеты заказа — страницами). PRIMARY KEY по
        -- order_id делает запись идемпотентной: повторная задача не создаст дубль.
        order_id    bigint      PRIMARY KEY REFERENCES orders (id),
        -- bytea: билеты — килобайты, хранить их рядом с заказом проще всего. При
        -- миллионах файлов или больших вложениях их выносят в S3/MinIO, а в базе
        -- оставляют ссылку.
        pdf         bytea       NOT NULL,
        created_at  timestamptz NOT NULL DEFAULT now()
    )
    """)


def downgrade() -> None:
    op.execute("DROP TABLE ticket_pdfs")
    op.execute("DROP INDEX orders_pdf_pending")
    op.execute("ALTER TABLE orders DROP COLUMN pdf_attempts, DROP COLUMN pdf_status")
