#!/bin/sh
# ТОЛЬКО для локального стенда: вернуть концерт в состояние "продажи только что
# открылись" перед нагрузочным тестом — пустой зал, пустая очередь, без заказов.
# Удаляет заказы, билеты и PDF из базы и всё состояние продаж из Redis.
#
#   sh loadtest/reset.sh
set -e
cd "$(dirname "$0")/.."

docker compose exec -T postgres psql -U ticket -d ticket -qc \
  "TRUNCATE ticket_pdfs, tickets, orders RESTART IDENTITY;
   UPDATE events SET sale_opens_at = now() - interval '1 minute'"

docker compose exec -T redis sh -c '
  for pattern in "q:*" "seats:*" "event:*" "ratelimit:*" "fakepay:*" "jobs:*"; do
    redis-cli --scan --pattern "$pattern" | xargs -r redis-cli del > /dev/null
  done'

echo "Зал пуст, очередь пуста, продажи открыты."
