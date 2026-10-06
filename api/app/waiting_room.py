"""Очередь ожидания ("виртуальный зал ожидания") в Redis.

Ключи одной очереди (на концерт):
  q:{e}:seq     счётчик номеров — INCR даёт каждому следующий номер атомарно
  q:{e}:line    sorted set  сессия -> номер: кто в очереди и под каким номером
  q:{e}:seen    sorted set  сессия -> когда последний раз подавала признаки жизни
  q:{e}:active  sorted set  сессия -> когда истекает пропуск к покупке
  q:{e}:upto    все номера <= этого уже прошли (впущены или пропущены)
  q:{e}:state   последний снимок состояния (его же рассылаем по WebSocket)

Оба изменяющих действия — Lua-скрипты: Redis выполняет скрипт атомарно, и между
"проверить" и "изменить" никто не вклинится. Отдельными командами два
одновременных входа одной сессии получили бы два номера.
"""

import json
import time
from dataclasses import dataclass

from redis.asyncio import Redis

STATE_CHANNEL = "queue:state"


def keys(event_id: int) -> dict[str, str]:
    base = f"q:{event_id}"
    return {
        name: f"{base}:{name}"
        for name in ("seq", "line", "seen", "active", "upto", "state", "sold_out")
    }


def now_ms() -> int:
    return int(time.time() * 1000)


# Вход в очередь. Повторный вход той же сессии возвращает её прежний номер —
# если она ещё ждёт или держит пропуск. А если её номер уже прошёл (пропуск истёк
# или её пропустили как ушедшую), она встаёт в КОНЕЦ: иначе, вернувшись, она
# обогнала бы всех, кто честно ждёт.
_JOIN = """
local seq_key, line, seen, active, upto_key = KEYS[1], KEYS[2], KEYS[3], KEYS[4], KEYS[5]
local sid, now = ARGV[1], tonumber(ARGV[2])
redis.call('ZADD', seen, now, sid)
local existing = redis.call('ZSCORE', line, sid)
if existing then
    local upto = tonumber(redis.call('GET', upto_key) or '0')
    local pass = redis.call('ZSCORE', active, sid)
    if tonumber(existing) > upto or (pass and tonumber(pass) > now) then
        return tonumber(existing)
    end
end
local seq = redis.call('INCR', seq_key)
redis.call('ZADD', line, seq, sid)
return seq
"""

# Впуск очередной порции. Выполняет только лидер-впускатель, и скрипт САМ
# проверяет, что вызывающий всё ещё лидер (fencing). Иначе старый лидер, который
# "проспал" потерю лидерства (пауза процесса, сетевой сбой), впускал бы людей
# параллельно с новым, и мест оказалось бы выдано больше ёмкости.
_ADMIT = """
local leader, line, seen, active, upto_key = KEYS[1], KEYS[2], KEYS[3], KEYS[4], KEYS[5]
local me, now = ARGV[1], tonumber(ARGV[2])
local capacity, max_admit = tonumber(ARGV[3]), tonumber(ARGV[4])
local pass_ttl, presence, max_scan = tonumber(ARGV[5]), tonumber(ARGV[6]), tonumber(ARGV[7])

if redis.call('GET', leader) ~= me then
    return {-1, 0, 0, 0}
end

-- Истёкшие пропуска освобождают места для следующих.
redis.call('ZREMRANGEBYSCORE', active, '-inf', now)
-- Как и ждущих, впущенных проверяем на присутствие: ушёл (закрыл вкладку, не
-- нашёл мест) и молчит дольше presence — пропуск отзываем. Без этого ушедший
-- держал бы место покупателя все 10 минут жизни пропуска, а очередь стояла бы
-- (нагрузочный тест: 200 из 200 пропусков у ушедших, впуск остановился).
-- Цена — проход по active, а он не больше ёмкости (сотни записей).
for _, sid in ipairs(redis.call('ZRANGE', active, 0, -1)) do
    local last_seen = redis.call('ZSCORE', seen, sid)
    if not last_seen or tonumber(last_seen) < now - presence then
        redis.call('ZREM', active, sid)
    end
end
local active_count = redis.call('ZCARD', active)
local slots = math.min(capacity - active_count, max_admit)
local upto = tonumber(redis.call('GET', upto_key) or '0')
local admitted, skipped, scanned = 0, 0, 0

-- max_scan ограничивает работу скрипта: пока он выполняется, Redis не отвечает
-- НИКОМУ. Если впереди тысячи ушедших, пропустим их за несколько тиков.
while slots > 0 and scanned < max_scan do
    local nxt = redis.call('ZRANGEBYSCORE', line, '(' .. upto, '+inf',
                           'WITHSCORES', 'LIMIT', 0, 1)
    if #nxt == 0 then break end
    local sid = nxt[1]
    upto = tonumber(nxt[2])
    scanned = scanned + 1
    local last_seen = redis.call('ZSCORE', seen, sid)
    if last_seen and tonumber(last_seen) >= now - presence then
        redis.call('ZADD', active, now + pass_ttl, sid)
        admitted = admitted + 1
        slots = slots - 1
    else
        skipped = skipped + 1
    end
end

redis.call('SET', upto_key, upto)
return {admitted, skipped, upto, active_count + admitted}
"""


@dataclass(frozen=True)
class AdmitResult:
    is_leader: bool
    admitted: int
    skipped: int
    admitted_upto: int
    active: int


class WaitingRoom:
    def __init__(self, redis: Redis) -> None:
        self._redis = redis
        self._join = redis.register_script(_JOIN)
        self._admit = redis.register_script(_ADMIT)

    async def join(self, event_id: int, session_id: str) -> int:
        k = keys(event_id)
        return int(
            await self._join(
                keys=[k["seq"], k["line"], k["seen"], k["active"], k["upto"]],
                args=[session_id, now_ms()],
            )
        )

    async def heartbeat(self, event_id: int, session_id: str) -> None:
        # Отмечаем только тех, кто в очереди: иначе любой открытый WebSocket
        # раздувал бы sorted set присутствия посторонними сессиями.
        k = keys(event_id)
        await self._redis.zadd(k["seen"], {session_id: now_ms()}, xx=True)

    async def status(self, event_id: int, session_id: str) -> dict:
        k = keys(event_id)
        async with self._redis.pipeline(transaction=False) as pipe:
            pipe.zscore(k["line"], session_id)
            pipe.zscore(k["active"], session_id)
            pipe.get(k["upto"])
            pipe.get(k["state"])
            pipe.zadd(k["seen"], {session_id: now_ms()}, xx=True)
            seq, pass_expires, upto, state, _ = await pipe.execute()

        snapshot = json.loads(state) if state else {}
        rate = snapshot.get("rate_per_second") or 0
        upto = int(upto or 0)
        body: dict = {
            "event_id": event_id,
            "admitted_upto": upto,
            "rate_per_second": rate,
            "sold_out": bool(snapshot.get("sold_out")),
        }
        if seq is None:
            return body | {"state": "not_in_queue"}
        seq = int(seq)
        if pass_expires is not None and pass_expires > now_ms():
            return body | {
                "state": "admitted",
                "seq": seq,
                "pass_expires_at": int(pass_expires) // 1000,
            }
        if seq <= upto:
            # Номер прошёл, а пропуска нет: он истёк, или сессию пропустили как
            # ушедшую. Повторный вход поставит в конец очереди.
            return body | {"state": "passed", "seq": seq}
        # Позиция — верхняя оценка: часть стоящих впереди ушла и будет пропущена.
        position = seq - upto
        return body | {
            "state": "waiting",
            "seq": seq,
            "position": position,
            "eta_seconds": round(position / rate) if rate else None,
        }

    async def admit(
        self,
        event_id: int,
        *,
        leader_key: str,
        leader_id: str,
        capacity: int,
        max_admit: int,
        pass_ttl_ms: int,
        presence_ms: int,
    ) -> AdmitResult:
        k = keys(event_id)
        admitted, skipped, upto, active = await self._admit(
            keys=[leader_key, k["line"], k["seen"], k["active"], k["upto"]],
            args=[leader_id, now_ms(), capacity, max_admit, pass_ttl_ms, presence_ms,
                  max_admit * 4],
        )
        return AdmitResult(admitted >= 0, max(admitted, 0), skipped, upto, active)

    async def mark_sold_out(self, event_id: int) -> None:
        # Флаг с TTL: живёт, пока впускатель его продлевает (каждый тик).
        await self._redis.set(keys(event_id)["sold_out"], 1, ex=60)

    async def is_sold_out(self, event_id: int) -> bool:
        return bool(await self._redis.exists(keys(event_id)["sold_out"]))

    async def current_progress(self, event_id: int) -> tuple[int, int]:
        """(впущено до номера, активных пропусков) — без впуска."""
        k = keys(event_id)
        async with self._redis.pipeline(transaction=False) as pipe:
            pipe.get(k["upto"])
            pipe.zcard(k["active"])
            upto, active = await pipe.execute()
        return int(upto or 0), int(active)

    async def joined_total(self, event_id: int) -> int:
        return int(await self._redis.get(keys(event_id)["seq"]) or 0)

    async def save_state(self, event_id: int, state: dict) -> None:
        payload = json.dumps(state)
        async with self._redis.pipeline(transaction=False) as pipe:
            # Снимок с TTL: если впускатель остановился, старое состояние не
            # будет вечно выдаваться за текущее.
            pipe.set(keys(event_id)["state"], payload, ex=10)
            pipe.publish(STATE_CHANNEL, payload)
            await pipe.execute()

    async def current_state(self, event_id: int) -> str | None:
        return await self._redis.get(keys(event_id)["state"])
