"""Оценка скорости впуска — основа ETA, который видят ждущие."""

from conftest import running_app


async def test_rate_not_lowered_when_queue_drains(settings, db, redis):
    async with running_app(settings) as client:
        admitter = client.app.state.admitter
        # Впустили одного, и очередь опустела: система не медленная, просто
        # больше никого нет. Оценка скорости не должна падать.
        assert admitter._measure_rate(1, admitted=1, waiting=0) == settings.admit_rate_per_second


async def test_rate_drops_when_capacity_limits_admission(settings, db, redis):
    async with running_app(settings) as client:
        admitter = client.app.state.admitter
        # Люди ждут, а впускать некуда (все места у покупателей) — ETA растёт.
        rates = [admitter._measure_rate(1, admitted=0, waiting=500) for _ in range(5)]
        assert rates == sorted(rates, reverse=True)
        assert rates[-1] < settings.admit_rate_per_second / 2
