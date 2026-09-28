"""Впускатель: раз в секунду пропускает следующую порцию очереди.

Кандидат живёт в КАЖДОМ воркере API, но работает только лидер (LeaderLease).
Так впуск не зависит от одной выделенной машины: упала реплика с лидером —
через leader_ttl лидером станет воркер другой реплики, очередь продолжит двигаться.

Каждый тик лидер публикует одно общее состояние очереди (впущены номера до N,
скорость впуска). Его получают все воркеры и раздают своим WebSocket-клиентам,
а позицию и время ожидания каждый клиент считает сам из своего номера. Одно
сообщение в секунду на концерт — вместо персонального каждому из тысяч ждущих.
"""

import asyncio
import logging
import os
import socket
import time
import uuid

from fastapi import FastAPI
from redis.exceptions import RedisError

from app.config import Settings
from app.deps import DatabaseUnavailable, db_for_app
from app.leader import LeaderLease
from app.waiting_room import WaitingRoom

log = logging.getLogger(__name__)

LEADER_KEY = "leader:admitter"
# Список концертов с открытыми продажами берём из базы не каждый тик, а раз в
# столько секунд: он меняется редко, а база не должна получать запрос в секунду
# от лидера просто так.
OPEN_EVENTS_REFRESH_SECONDS = 10
# Сглаживание измеренной скорости впуска: новое значение весит 30%. Так ETA не
# скачет от тика к тику, но быстро реагирует на замедление (кончилась ёмкость).
RATE_SMOOTHING = 0.3


class Admitter:
    def __init__(self, app: FastAPI, room: WaitingRoom, settings: Settings) -> None:
        self._app = app
        self._room = room
        self._settings = settings
        identity = f"{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:8]}"
        self.lease = LeaderLease(
            app.state.redis, LEADER_KEY, identity, int(settings.leader_ttl_seconds * 1000)
        )
        self.is_leader = False
        self._rates: dict[int, float] = {}
        self._open_events: list[int] = []
        self._open_events_at = 0.0
        self._task: asyncio.Task | None = None

    def start(self) -> None:
        self._task = asyncio.create_task(self._run(), name="admitter")

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        if self.is_leader:
            # Отдаём лидерство сразу, а не ждём истечения аренды: при деплое
            # очередь не простаивает leader_ttl секунд.
            try:
                await self.lease.release()
            except RedisError:
                pass

    async def _run(self) -> None:
        while True:
            try:
                await self.step()
            except asyncio.CancelledError:
                raise
            except (RedisError, DatabaseUnavailable, OSError):
                # Без Redis нельзя ни продлить аренду, ни впускать. Считаем себя
                # не лидером: даже если аренда ещё жива, fencing в скрипте
                # впуска не даст натворить лишнего.
                if self.is_leader:
                    log.warning("admitter_lost_leadership_on_error", exc_info=True)
                self.is_leader = False
            await asyncio.sleep(self._settings.admitter_tick_seconds)

    async def step(self) -> None:
        """Один тик: удержать/получить лидерство и, если лидер, впустить."""
        if self.is_leader:
            self.is_leader = await self.lease.renew()
            if not self.is_leader:
                log.warning("admitter_lost_leadership", extra={"leader_id": self.lease.identity})
        else:
            self.is_leader = await self.lease.acquire()
            if self.is_leader:
                log.info("admitter_became_leader", extra={"leader_id": self.lease.identity})
        if self.is_leader:
            for event_id in await self._events_on_sale():
                await self._tick(event_id)

    async def _tick(self, event_id: int) -> None:
        s = self._settings
        result = await self._room.admit(
            event_id,
            leader_key=LEADER_KEY,
            leader_id=self.lease.identity,
            capacity=s.admit_capacity,
            max_admit=max(1, round(s.admit_rate_per_second * s.admitter_tick_seconds)),
            pass_ttl_ms=s.pass_ttl_seconds * 1000,
            presence_ms=s.presence_timeout_seconds * 1000,
        )
        if not result.is_leader:
            # Fencing сработал: аренду уже держит другой. Работу прекращаем.
            self.is_leader = False
            log.warning("admitter_fenced", extra={"event_id": event_id})
            return

        joined = await self._room.joined_total(event_id)
        waiting = max(joined - result.admitted_upto, 0)
        rate = self._measure_rate(event_id, result.admitted, waiting)
        await self._room.save_state(
            event_id,
            {
                "event_id": event_id,
                "admitted_upto": result.admitted_upto,
                "joined": joined,
                "waiting": waiting,
                "active": result.active,
                "capacity": s.admit_capacity,
                "rate_per_second": round(rate, 1),
                "ts": int(time.time()),
            },
        )
        if result.admitted or result.skipped:
            log.info(
                "queue_admitted",
                extra={
                    "event_id": event_id,
                    "admitted": result.admitted,
                    "skipped": result.skipped,
                    "admitted_upto": result.admitted_upto,
                    "active": result.active,
                },
            )

    def _measure_rate(self, event_id: int, admitted: int, waiting: int) -> float:
        # Реальная скорость впуска, а не настроечная: когда покупатели заняли всю
        # ёмкость, впуск замедляется, и ETA должен это честно показать.
        per_second = admitted / self._settings.admitter_tick_seconds
        previous = self._rates.get(event_id, float(self._settings.admit_rate_per_second))
        if waiting == 0:
            # Очередь опустела: впустили столько, сколько было людей, а не
            # столько, сколько позволяет система. Это не скорость впуска — иначе
            # один пришедший в пустую очередь "замедлял" бы оценку для всех
            # следующих. Сохраняем прежнюю.
            return previous
        rate = previous * (1 - RATE_SMOOTHING) + per_second * RATE_SMOOTHING
        self._rates[event_id] = rate
        return rate

    async def _events_on_sale(self) -> list[int]:
        if time.monotonic() - self._open_events_at > OPEN_EVENTS_REFRESH_SECONDS:
            async with db_for_app(self._app) as connection:
                rows = await connection.fetch(
                    "SELECT id FROM events WHERE sale_opens_at <= now() AND starts_at > now()"
                )
            self._open_events = [row["id"] for row in rows]
            self._open_events_at = time.monotonic()
        return self._open_events
