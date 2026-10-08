#!/bin/sh
# Проверка инвариантов после нагрузочного теста — по самим данным, а не по
# ответам сервера. Выход с ошибкой, если хоть один нарушен. Используется в CI.
#
#   sh loadtest/check_invariants.sh
set -e
cd "$(dirname "$0")/.."

sql() { docker compose exec -T postgres psql -U ticket -d ticket -tAc "$1"; }

# PDF генерируются в фоне — даём воркерам дорендерить начатое.
for _ in $(seq 1 30); do
  [ "$(sql "SELECT count(*) FROM orders WHERE status = 'paid' AND pdf_status = 'pending'")" = "0" ] && break
  sleep 2
done

fail=0
check() {
  if [ "$2" = "$3" ]; then
    echo "ok   $1: $2"
  else
    echo "FAIL $1: $2 (ожидалось $3)"
    fail=1
  fi
}

paid=$(sql "SELECT count(*) FROM orders WHERE status = 'paid'")
echo "оплачено заказов: $paid"
[ "$paid" -gt 0 ] || { echo "FAIL ни одного оплаченного заказа — сценарий не дошёл до покупки"; exit 1; }

check "мест с двумя билетами" "$(sql "SELECT count(*) FROM (SELECT 1 FROM tickets GROUP BY event_id, seat_id HAVING count(*) > 1) d")" 0
check "заказов на один ключ больше одного" "$(sql "SELECT count(*) FROM (SELECT 1 FROM orders GROUP BY session_id, idempotency_key HAVING count(*) > 1) d")" 0
check "оплаченных заказов с неверным числом билетов" "$(sql "SELECT count(*) FROM orders o WHERE status = 'paid' AND cardinality(seat_ids) <> (SELECT count(*) FROM tickets t WHERE t.order_id = o.id)")" 0
check "оплаченных заказов без PDF" "$(sql "SELECT count(*) FROM orders WHERE status = 'paid' AND pdf_status <> 'ready'")" 0
check "списаний денег = оплаченных заказов" "$(docker compose exec -T redis redis-cli HLEN fakepay:charges)" "$paid"
check "продано в Redis = билетов в базе" "$(docker compose exec -T redis redis-cli SCARD seats:1:sold)" "$(sql "SELECT count(*) FROM tickets")"

exit $fail
