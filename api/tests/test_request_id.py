import json
import logging

from conftest import running_app

from app.logging_config import JsonFormatter, request_id_var


async def test_request_id_from_nginx_is_returned(settings, db, redis):
    async with running_app(settings) as client:
        response = await client.get(
            "/api/debug/instance-info", headers={"X-Request-ID": "abc123def456"}
        )
        assert response.headers["x-request-id"] == "abc123def456"


async def test_request_id_generated_when_missing_or_invalid(settings, db, redis):
    async with running_app(settings) as client:
        missing = await client.get("/api/debug/instance-info")
        invalid = await client.get(
            "/api/debug/instance-info", headers={"X-Request-ID": "<script>alert(1)</script>"}
        )
        assert len(missing.headers["x-request-id"]) == 32
        assert invalid.headers["x-request-id"] != "<script>alert(1)</script>"


def test_log_line_contains_request_id():
    token = request_id_var.set("req-000111222")
    try:
        record = logging.LogRecord("app", logging.INFO, __file__, 1, "seat_held", None, None)
        record.event_id = 7
        entry = json.loads(JsonFormatter().format(record))
    finally:
        request_id_var.reset(token)

    assert entry["event"] == "seat_held"
    assert entry["request_id"] == "req-000111222"
    assert entry["event_id"] == 7
    assert entry["instance_hostname"]
