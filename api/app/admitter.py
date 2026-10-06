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
from app.seats import SeatStore
from app.ticket_jobs import enqueue_ticket_pdf
from app.waiting_room import WaitingRoom

log = logging.getLogger(__name__)

LEADER_KEY = "leader:admitter"
# Список концертов с открытыми продажами берём из базы не каждый тик, а раз в
# столько секунд: он меняется редко, а база не должна получать запрос в секунду
# от лидера просто так.
OPEN_EVENTS_REFRESH_SECONDS = 10
# Сверка проданных мест база -> Redis и уборка зависших заказов: редкие фоновые
# работы, которым достаточно одного исполнителя на кластер — того же лидера.
RECONCILE_SECONDS = 10
ABANDON_CHECK_SECONDS = 30
# Сглаживание измеренной скорости впуска: новое значение весит 30%. Так ETA не
# скачет от тика к тику, но быстро реагирует на замедление (кончилась ёмкость).
RATE_SMOOTHING = 0.3


class Admitter:
    def __init__(
        self, app: FastAPI, room: WaitingRoom, seats: SeatStore, settings: Settings
    ) -> None:
        self._app = app
        self._room = room
        self._seats = seats
        self._settings = settings
        identity = f"{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:8]}"
        self.lease = LeaderLease(
            app.state.redis, LEADER_KEY, identity, int(settings.leader_ttl_seconds * 1000)
        )
        self.is_leader = False
        self._rates: dict[int, float] = {}
        self._open_events: list[int] = []
        self._hall_size: dict[int, int] = {}
        self._open_events_at = 0.0
        self._reconciled_at = 0.0
        self._abandon_checked_at = 0.0
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
                # Истёкшие брони освобождает тот же лидер: раз в тик, а не
                # механизм истечения ключей Redis — тот удаляет ключи лениво, и
                # "место освободилось" приходило бы зрителям непредсказуемо поздно.
                freed = await self._seats.sweep_expired(event_id)
                if freed:
                    log.info("holds_expired", extra={"event_id": event_id, "seats": freed})
            await self._maintenance()

    async def _tick(self, event_id: int) -> None:
        s = self._settings
        # Всё продано — впускать незачем, а ждущим нужно об этом СКАЗАТЬ. Без этого
        # (нагрузочный тест) 1935 человек ждали в очереди после того, как все 2000
        # мест были проданы. Проданное в Redis отстаёт от базы не больше чем на
        # сверку, а флаг с TTL снимется сам, если места вернутся (возврат билета).
        sold_out = await self._seats.sold_count(event_id) >= self._hall_size.get(event_id, 1 << 30)
        if sold_out:
            await self._room.mark_sold_out(event_id)
            upto, active = await self._room.current_progress(event_id)
            joined = await self._room.joined_total(event_id)
            await self._room.save_state(event_id, self._state(
                event_id, upto, joined, active, 0.0, sold_out=True))
            return

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
            event_id, self._state(event_id, result.admitted_upto, joined, result.active, rate)
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

    def _state(
        self, event_id: int, upto: int, joined: int, active: int, rate: float,
        sold_out: bool = False,
    ) -> dict:
        return {
            # Тип нужен клиенту: по одному WebSocket приходят и состояние очереди,
            # и изменения схемы зала.
            "type": "queue",
            "event_id": event_id,
            "admitted_upto": upto,
            "joined": joined,
            "waiting": max(joined - upto, 0),
            "active": active,
            "capacity": self._settings.admit_capacity,
            "rate_per_second": round(rate, 1),
            "sold_out": sold_out,
            "ts": int(time.time()),
        }

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

    async def _maintenance(self) -> None:
        now = time.monotonic()
        if now - self._reconciled_at > RECONCILE_SECONDS:
            self._reconciled_at = now
            for event_id in self._open_events:
                await self.reconcile_sold(event_id)
        if now - self._abandon_checked_at > ABANDON_CHECK_SECONDS:
            self._abandon_checked_at = now
            await self.abandon_stale_orders()
            await self.requeue_missing_pdfs()

    async def reconcile_sold(self, event_id: int) -> int:
        """Проданные места из базы (источник правды) — в Redis, если их там нет.

        Покупка пишет сначала в базу, потом в Redis; если вторая запись не
        удалась (или Redis перезапустился без данных), без сверки место выглядело
        бы свободным, и его можно было бы забронировать — покупка бы потом
        упёрлась в UNIQUE базы, но человек зря потратил бы время.
        """
        async with db_for_app(self._app) as connection:
            rows = await connection.fetch(
                "SELECT seat_id FROM tickets WHERE event_id = $1", event_id
            )
        restored = await self._seats.restore_sold(event_id, [row["seat_id"] for row in rows])
        if restored:
            log.warning("redis_sold_restored", extra={"event_id": event_id, "seats": restored})
        return restored

    async def abandon_stale_orders(self) -> list[int]:
        """Заказы, зависшие в 'pending' (процесс упал посреди оплаты), — закрыть
        и вернуть деньги, если они успели списаться. Иначе деньги висели бы у нас
        без билета, а место — в неопределённости."""
        async with db_for_app(self._app) as connection:
            rows = await connection.fetch(
                """
                UPDATE orders SET status = 'failed', failure_reason = 'abandoned',
                                  updated_at = now()
                WHERE status = 'pending'
                  AND created_at < now() - make_interval(secs => $1)
                RETURNING id
                """,
                self._settings.pending_order_timeout_seconds,
            )
        abandoned = [row["id"] for row in rows]
        for order_id in abandoned:
            refunded = await self._app.state.payments.refund(order_id)
            log.warning("order_abandoned", extra={"order_id": order_id, "refunded": refunded})
        return abandoned

    async def requeue_missing_pdfs(self) -> list[int]:
        """Оплаченные заказы без PDF дольше 5 минут — снова в очередь генерации.

        Покупка ставит задачу ПОСЛЕ записи в базу; если постановка не удалась,
        без этой сверки заказ остался бы без билета навсегда. updated_at
        сдвигается, чтобы не ставить ту же задачу каждые 30 секунд.
        """
        async with db_for_app(self._app) as connection:
            rows = await connection.fetch(
                """
                UPDATE orders SET updated_at = now()
                WHERE id IN (
                    SELECT id FROM orders
                    WHERE status = 'paid' AND pdf_status = 'pending'
                      AND updated_at < now() - interval '5 minutes'
                    LIMIT 100
                )
                RETURNING id
                """
            )
        for row in rows:
            await enqueue_ticket_pdf(self._app.state.redis, row["id"])
        if rows:
            log.warning("ticket_pdfs_requeued", extra={"orders": len(rows)})
        return [row["id"] for row in rows]

    async def _events_on_sale(self) -> list[int]:
        if time.monotonic() - self._open_events_at > OPEN_EVENTS_REFRESH_SECONDS:
            async with db_for_app(self._app) as connection:
                rows = await connection.fetch(
                    "SELECT e.id, count(s.id) AS seats FROM events e "
                    "JOIN seats s ON s.event_id = e.id "
                    "WHERE e.sale_opens_at <= now() AND e.starts_at > now() GROUP BY e.id"
                )
            self._open_events = [row["id"] for row in rows]
            self._hall_size = {row["id"]: row["seats"] for row in rows}
            self._open_events_at = time.monotonic()
        return self._open_events
