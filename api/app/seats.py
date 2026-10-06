"""Бронь мест в Redis.

Ключи одного концерта:
  seats:{e}:held       sorted set  место -> когда истекает бронь (мс)
  seats:{e}:owner      hash        место -> сессия, которая его держит
  seats:{e}:by:{sid}   set         места сессии (для лимита "не больше N в руки")
  seats:{e}:sold       set         проданные места (заполняется на этапе 5)
  event:{e}:seatmap:version        номер версии схемы зала

Каждое изменение статусов увеличивает версию на 1 и публикуется в канал
seats:changes ПРЯМО ИЗ Lua-скрипта. Скрипты выполняются строго по одному, поэтому
порядок публикаций совпадает с порядком версий. Публикуй мы из Python после
скрипта, два одновременных бронирования могли бы разослать версии 57 и 56 в
обратном порядке — и клиенты видели бы ложные "пропуски" версий.
"""

import asyncio

from redis.asyncio import Redis

from app.waiting_room import keys as queue_keys
from app.waiting_room import now_ms

CHANGES_CHANNEL = "seats:changes"


def keys(event_id: int, session_id: str = "") -> dict[str, str]:
    base = f"seats:{event_id}"
    return {
        "held": f"{base}:held",
        "owner": f"{base}:owner",
        "by": f"{base}:by:{session_id}",
        "sold": f"{base}:sold",
        "version": f"event:{event_id}:seatmap:version",
    }


# Бронь: всё или ничего. Если хоть одно место занято чужой бронью или продано,
# не бронируется ни одно — пара мест рядом это одна покупка, а не две.
_HOLD = """
local held, owner, by, sold, version, active = KEYS[1], KEYS[2], KEYS[3], KEYS[4], KEYS[5], KEYS[6]
local sid, now, ttl, max_seats = ARGV[1], tonumber(ARGV[2]), tonumber(ARGV[3]), tonumber(ARGV[4])
local event_id = tonumber(ARGV[5])

local pass = redis.call('ZSCORE', active, sid)
if not pass or tonumber(pass) <= now then
    return {'no_pass'}
end

-- Текущие брони сессии. Из набора убираем истёкшие и перехваченные другими:
-- набор — индекс для лимита, источник правды — held/owner.
local current, count = {}, 0
for _, seat in ipairs(redis.call('SMEMBERS', by)) do
    local expires = redis.call('ZSCORE', held, seat)
    if expires and tonumber(expires) > now and redis.call('HGET', owner, seat) == sid then
        current[seat] = true
        count = count + 1
    else
        redis.call('SREM', by, seat)
    end
end

local new, taken = {}, {}
for i = 6, #ARGV do
    local seat = ARGV[i]
    if not current[seat] then
        local expires = redis.call('ZSCORE', held, seat)
        if redis.call('SISMEMBER', sold, seat) == 1
           or (expires and tonumber(expires) > now) then
            table.insert(taken, seat)
        else
            table.insert(new, seat)
        end
    end
end
if #taken > 0 then
    return {'taken', unpack(taken)}
end
if count + #new > max_seats then
    return {'too_many', tostring(count)}
end

-- У всех мест сессии один срок: одна покупка — один таймер.
local expires_at = now + ttl
for seat, _ in pairs(current) do
    redis.call('ZADD', held, expires_at, seat)
end
local changes = {}
for _, seat in ipairs(new) do
    redis.call('ZADD', held, expires_at, seat)
    redis.call('HSET', owner, seat, sid)
    redis.call('SADD', by, seat)
    table.insert(changes, {tonumber(seat), 'held'})
end
redis.call('PEXPIRE', by, ttl)

local v = redis.call('GET', version) or '0'
if #new > 0 then
    v = redis.call('INCR', version)
    redis.call('PUBLISH', 'seats:changes', cjson.encode(
        {type = 'seats', event_id = event_id, version = v, changes = changes}))
end
return {'ok', tostring(v), tostring(expires_at)}
"""

# Снять свою бронь. Чужую — нельзя (проверка владельца внутри скрипта).
_RELEASE = """
local held, owner, by, version = KEYS[1], KEYS[2], KEYS[3], KEYS[4]
local sid, seat, now, event_id = ARGV[1], ARGV[2], tonumber(ARGV[3]), tonumber(ARGV[4])
local expires = redis.call('ZSCORE', held, seat)
if not expires or tonumber(expires) <= now or redis.call('HGET', owner, seat) ~= sid then
    return 0
end
redis.call('ZREM', held, seat)
redis.call('HDEL', owner, seat)
redis.call('SREM', by, seat)
local v = redis.call('INCR', version)
redis.call('PUBLISH', 'seats:changes', cjson.encode(
    {type = 'seats', event_id = event_id, version = v, changes = {{tonumber(seat), 'free'}}}))
return v
"""

# Освободить истёкшие брони и сообщить об этом зрителям. Идемпотентно: второй
# вызов подряд ничего не найдёт, поэтому fencing здесь не нужен — два
# "лидера" сделали бы одну и ту же безвредную работу.
_SWEEP = """
local held, owner, version = KEYS[1], KEYS[2], KEYS[3]
local now, limit, event_id = tonumber(ARGV[1]), tonumber(ARGV[2]), tonumber(ARGV[3])
local expired = redis.call('ZRANGEBYSCORE', held, '-inf', now, 'LIMIT', 0, limit)
if #expired == 0 then
    return 0
end
local changes = {}
for _, seat in ipairs(expired) do
    redis.call('ZREM', held, seat)
    redis.call('HDEL', owner, seat)
    table.insert(changes, {tonumber(seat), 'free'})
end
local v = redis.call('INCR', version)
redis.call('PUBLISH', 'seats:changes', cjson.encode(
    {type = 'seats', event_id = event_id, version = v, changes = changes}))
return #expired
"""


# Перед оплатой: проверить, что сессия держит РОВНО эти места и брони живы, и
# продлить их на время платежа. Без продления бронь могла бы истечь, пока банк
# думает, и место ушло бы другому покупателю, за которое мы уже взяли деньги.
_LOCK_FOR_PAYMENT = """
local held, owner, by = KEYS[1], KEYS[2], KEYS[3]
local sid, now, window = ARGV[1], tonumber(ARGV[2]), tonumber(ARGV[3])
local requested = {}
for i = 4, #ARGV do requested[ARGV[i]] = true end

local mine = 0
for _, seat in ipairs(redis.call('SMEMBERS', by)) do
    local expires = redis.call('ZSCORE', held, seat)
    if expires and tonumber(expires) > now and redis.call('HGET', owner, seat) == sid then
        if not requested[seat] then return 'mismatch' end
        mine = mine + 1
    end
end
if mine ~= #ARGV - 3 then return 'mismatch' end

for seat, _ in pairs(requested) do
    local expires = tonumber(redis.call('ZSCORE', held, seat))
    if expires < now + window then
        redis.call('ZADD', held, now + window, seat)
    end
end
return 'ok'
"""

# После того как билеты записаны в базу: места проданы, брони и пропуск сняты
# (пропуск освобождает место для следующего в очереди), зрителям — изменение.
_MARK_SOLD = """
local held, owner, by, sold, version, active = KEYS[1], KEYS[2], KEYS[3], KEYS[4], KEYS[5], KEYS[6]
local sid, event_id = ARGV[1], tonumber(ARGV[2])
local changes = {}
for i = 3, #ARGV do
    local seat = ARGV[i]
    redis.call('SADD', sold, seat)
    redis.call('ZREM', held, seat)
    redis.call('HDEL', owner, seat)
    redis.call('SREM', by, seat)
    table.insert(changes, {tonumber(seat), 'sold'})
end
redis.call('ZREM', active, sid)
local v = redis.call('INCR', version)
redis.call('PUBLISH', 'seats:changes', cjson.encode(
    {type = 'seats', event_id = event_id, version = v, changes = changes}))
return v
"""

# Сверка с базой: проданные места, которых нет в Redis (запись в Redis после
# покупки не удалась, Redis перезапустился без данных), — добавить и разослать.
_RESTORE_SOLD = """
local held, owner, sold, version = KEYS[1], KEYS[2], KEYS[3], KEYS[4]
local event_id = tonumber(ARGV[1])
local changes = {}
for i = 2, #ARGV do
    local seat = ARGV[i]
    if redis.call('SADD', sold, seat) == 1 then
        redis.call('ZREM', held, seat)
        redis.call('HDEL', owner, seat)
        table.insert(changes, {tonumber(seat), 'sold'})
    end
end
if #changes == 0 then return 0 end
local v = redis.call('INCR', version)
redis.call('PUBLISH', 'seats:changes', cjson.encode(
    {type = 'seats', event_id = event_id, version = v, changes = changes}))
return #changes
"""


class HoldError(Exception):
    def __init__(self, reason: str, seats: list[int] | None = None) -> None:
        super().__init__(reason)
        self.reason = reason
        self.seats = seats or []


class SeatStore:
    def __init__(self, redis: Redis) -> None:
        self._redis = redis
        self._hold = redis.register_script(_HOLD)
        self._release = redis.register_script(_RELEASE)
        self._sweep = redis.register_script(_SWEEP)
        self._lock_for_payment = redis.register_script(_LOCK_FOR_PAYMENT)
        self._mark_sold = redis.register_script(_MARK_SOLD)
        self._restore_sold = redis.register_script(_RESTORE_SOLD)

    async def lock_for_payment(
        self, event_id: int, session_id: str, seat_ids: list[int], window_ms: int
    ) -> bool:
        """True — сессия держит ровно эти места, брони продлены на время оплаты."""
        k = keys(event_id, session_id)
        result = await self._lock_for_payment(
            keys=[k["held"], k["owner"], k["by"]],
            args=[session_id, now_ms(), window_ms, *seat_ids],
        )
        return result == "ok"

    async def mark_sold(self, event_id: int, session_id: str, seat_ids: list[int]) -> int:
        k = keys(event_id, session_id)
        return int(
            await self._mark_sold(
                keys=[k["held"], k["owner"], k["by"], k["sold"], k["version"],
                      queue_keys(event_id)["active"]],
                args=[session_id, event_id, *seat_ids],
            )
        )

    async def restore_sold(self, event_id: int, seat_ids: list[int]) -> int:
        """Сколько проданных мест не хватало в Redis (и теперь добавлено)."""
        if not seat_ids:
            return 0
        k = keys(event_id)
        return int(
            await self._restore_sold(
                keys=[k["held"], k["owner"], k["sold"], k["version"]],
                args=[event_id, *seat_ids],
            )
        )

    async def hold(
        self, event_id: int, session_id: str, seat_ids: list[int], ttl_ms: int, max_seats: int
    ) -> tuple[int, int]:
        """Забронировать места. Возвращает (версия схемы, когда истекает бронь в мс)."""
        k = keys(event_id, session_id)
        result = await self._hold(
            keys=[k["held"], k["owner"], k["by"], k["sold"], k["version"],
                  queue_keys(event_id)["active"]],
            args=[session_id, now_ms(), ttl_ms, max_seats, event_id, *seat_ids],
        )
        status, *rest = result
        if status == "ok":
            return int(rest[0]), int(rest[1])
        raise HoldError(status, [int(seat) for seat in rest] if status == "taken" else None)

    async def release(self, event_id: int, session_id: str, seat_id: int) -> bool:
        k = keys(event_id, session_id)
        version = await self._release(
            keys=[k["held"], k["owner"], k["by"], k["version"]],
            args=[session_id, seat_id, now_ms(), event_id],
        )
        return bool(version)

    async def sweep_expired(self, event_id: int, limit: int = 500) -> int:
        k = keys(event_id)
        return int(
            await self._sweep(
                keys=[k["held"], k["owner"], k["version"]], args=[now_ms(), limit, event_id]
            )
        )

    async def sold_count(self, event_id: int) -> int:
        return int(await self._redis.scard(keys(event_id)["sold"]))

    async def my_holds(self, event_id: int, session_id: str) -> tuple[list[int], int | None]:
        """Места сессии и общий срок их брони (мс)."""
        k = keys(event_id, session_id)
        seats = await self._redis.smembers(k["by"])
        if not seats:
            return [], None
        async with self._redis.pipeline(transaction=False) as pipe:
            for seat in seats:
                pipe.zscore(k["held"], seat)
                pipe.hget(k["owner"], seat)
            answers = await pipe.execute()
        now = now_ms()
        mine = []
        expires_at = None
        for seat, expires, owner in zip(seats, answers[::2], answers[1::2], strict=True):
            if expires is not None and expires > now and owner == session_id:
                mine.append(int(seat))
                expires_at = int(expires)
        return sorted(mine), expires_at

    async def snapshot(self, event_id: int) -> dict:
        """Точные статусы мест прямо сейчас (для WebSocket) — с версией.

        Версия читается в той же транзакции MULTI/EXEC, что и статусы: снимок и
        его версия согласованы, между чтениями ничего не могло измениться.
        """
        k = keys(event_id)
        async with self._redis.pipeline(transaction=True) as pipe:
            pipe.get(k["version"])
            pipe.zrangebyscore(k["held"], f"({now_ms()}", "+inf")
            pipe.smembers(k["sold"])
            version, held, sold = await pipe.execute()
        return {
            "type": "seats_snapshot",
            "event_id": event_id,
            "version": int(version or 0),
            "held": sorted(int(seat) for seat in held),
            "sold": sorted(int(seat) for seat in sold),
        }


class SeatCatalog:
    """Какие места есть у концерта — в памяти воркера, навсегда.

    Состав зала не меняется, а проверять "это место этого концерта?" нужно на
    каждой брони. Запрос к базе на каждый клик в момент открытия продаж —
    лишняя нагрузка; 2000 чисел в памяти — ничто.
    """

    def __init__(self) -> None:
        self._seats: dict[int, frozenset[int]] = {}
        self._lock = asyncio.Lock()

    async def seat_ids(self, connection_factory, event_id: int) -> frozenset[int]:
        if event_id in self._seats:
            return self._seats[event_id]
        async with self._lock:
            if event_id not in self._seats:
                async with connection_factory() as connection:
                    rows = await connection.fetch(
                        "SELECT id FROM seats WHERE event_id = $1", event_id
                    )
                self._seats[event_id] = frozenset(row["id"] for row in rows)
        return self._seats[event_id]

