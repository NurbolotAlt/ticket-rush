# ticket-rush

[![CI](https://github.com/NurbolotAlt/ticket-rush/actions/workflows/ci.yml/badge.svg)](https://github.com/NurbolotAlt/ticket-rush/actions/workflows/ci.yml)

Продажа билетов на концерт, которая выдерживает **открытие продаж**: тысячи людей
приходят в одну секунду, а мест меньше, чем желающих. Виртуальная очередь
ожидания, бронь мест в реальном времени, оплата без двойных списаний и без
двойных продаж, PDF-билеты с QR-кодом.

FastAPI за nginx · PostgreSQL (источник правды) · Redis (очередь, брони, кэш,
события) · WebSocket · фоновые воркеры на Redis Streams · k6.

## Результаты

Нагрузочный тест [loadtest/journey.js](loadtest/journey.js): **3000 человек** за
10 секунд проходят весь путь — страница, очередь, выбор мест по живой схеме
зала, бронь, оплата (10% кликают дважды), PDF. Мест 2000.

| | |
|---|---|
| Распродажа 2000 мест | **1 мин 24 с** |
| Время от прихода до билета, p50 | 41 с (почти всё — честное ожидание в очереди) |
| Оплата, p50 · оплата → PDF, p50 | 177 мс · 59 мс |
| Мест с двумя билетами · двойных списаний | **0 · 0** |
| Неожиданных ошибок | **0** |

Тест нашёл две продуктовые ошибки (распродано, а очередь ждёт; ушедшие держат
места покупателей) и узкое место по профилю `py-spy` (рассылка по WebSocket) —
всё исправлено. Подробно, с таблицей «до/после» и оговорками об измерении на одной
машине: [docs/load-testing.md](docs/load-testing.md).

## Архитектура

```
браузер ──HTTPS──► nginx ─┬─ статика
                          ├─ каталог, схема зала ── микрокэш 1–5 с (proxy_cache_lock):
                          │                         тысячи запросов → 1 запрос к API
                          ├─ /api/*  ─► api × N (FastAPI, gunicorn)
                          └─ /ws/*   ─► api × N (один WebSocket на вкладку)
                                          │
              ┌───────────────────────────┼──────────────────────────────┐
              ▼                           ▼                              ▼
     PostgreSQL — правда           Redis — горячее               worker × N
     концерты, места,              очередь (sorted set),         Redis Streams →
     заказы, билеты,               брони (Lua), лимиты,          PDF + QR в пуле
     UNIQUE(event, seat)           Pub/Sub, лидерство            процессов
```

## Ключевые решения

- **Очередь ожидания.** Номер — атомарный `INCR`, впуск — Lua-скрипт: не быстрее
  50 человек в секунду и не больше 200 покупателей одновременно. Впускатель —
  ровно один на кластер (лидерство через аренду в Redis), а скрипт впуска сам
  проверяет «я ещё лидер?» (fencing). Позицию и ETA каждый браузер считает сам из
  одного общего сообщения в секунду. [waiting_room.py](api/app/waiting_room.py),
  [admitter.py](api/app/admitter.py)
- **Бронь «всё или ничего»** одним Lua-скриптом, с версией схемы зала. Изменения
  публикуются прямо из скрипта (порядок версий = порядок публикаций), склеиваются
  в пакеты по 150 мс; клиент сверяет версии и при пропуске берёт свежий снимок.
  [seats.py](api/app/seats.py), [realtime.py](api/app/realtime.py)
- **Покупка, устойчивая к сбоям посередине.** Заказ `pending` с ключом
  идемпотентности → бронь продлена → платёж (идемпотентный) → одна транзакция:
  билеты + `paid` → Redis. `UNIQUE(event_id, seat_id)` в базе — последняя защита от
  двойной продажи, даже если Redis ошибся. Соединение с базой во время платежа не
  держится. Зависшие заказы — возврат денег. [orders.py](api/app/orders.py)
- **Фоновые PDF.** Consumer group на Redis Streams, `XAUTOCLAIM` забирает задачи
  упавшего воркера, «отравленные» — в dead letter, обработка идемпотентна.
  [worker.py](api/app/worker.py)
- **База — источник правды, Redis — восстановимая копия.** Лидер сверяет проданные
  места и PDF с базой и чинит расхождения.
- **nginx делает большую часть работы:** статика, микрокэш каталога со
  stale-if-error (каталог виден даже при упавшем API), лимиты, TLS (ECDSA),
  отдельный upstream для WebSocket, `resolve` реплик. [nginx.conf](nginx/nginx.conf)
- **Безопасность:** подписанные cookie сессий (HMAC, HttpOnly, SameSite), лимиты
  по сессии и IP, персональные ответы `no-store`, процессы не от root, секреты
  только в `.env`.

## Запуск

```bash
cp .env.example .env      # задать POSTGRES_PASSWORD и SESSION_SECRET (openssl rand -hex 32)
docker compose up --build --scale api=2 --scale worker=2
```

https://localhost/ — сертификат self-signed, браузер предупредит. Продажи
открываются через 10 минут после первого запуска (`SEED_SALE_OPENS_IN_MINUTES`,
отрицательное — сразу).

**Тесты** — 84 интеграционных теста на настоящих PostgreSQL и Redis (Testcontainers,
нужен Docker):

```bash
pip install -r api/requirements.txt -r requirements-dev.txt   # Linux / CI
pip install -r api/requirements.in  -r requirements-dev.in    # Windows / macOS
ruff check api && cd api && python -m pytest
```

**Нагрузочный тест** — команды в [docs/load-testing.md](docs/load-testing.md).

## CI

[GitHub Actions](.github/workflows/ci.yml): ruff и pytest → Trivy → сборка образов →
**e2e**: весь стек в compose, k6 «открытие продаж» на 200 человек и проверка
инвариантов в базе ([check_invariants.sh](loadtest/check_invariants.sh)): ни одного
места с двумя билетами, списаний столько же, сколько оплаченных заказов, у каждого
заказа есть PDF. Зависимости обновляет Dependabot.

## Структура

```
api/app/          main, catalog, queue_routes, holds_routes, orders_routes — HTTP/WS
                  waiting_room, admitter, leader — очередь и впуск
                  seats, realtime — брони и живая схема зала
                  orders, payments — покупка
                  worker, ticket_jobs, ticket_pdf — фоновые PDF
api/migrations/   alembic на чистом SQL
api/tests/        интеграционные тесты (Testcontainers)
nginx/            конфиг, Dockerfile, сертификат
static/           фронтенд без фреймворков
loadtest/         k6-сценарии, сброс стенда, проверка инвариантов
docs/             результаты нагрузочного тестирования
```

## Лицензия

[MIT](LICENSE)
