import json

import httpx
import pytest
from conftest import message

from app.ai_client import AIClient, AIError, validate_alert
from app.bot import Bot
from app.search import search

ALERT = {
    "relevant": True,
    "needs_alert": True,
    "importance": 85,
    "confidence": 0.9,
    "category": "sale",
    "title": "测试优惠",
    "summary": "待核实",
}


@pytest.mark.parametrize(
    "field,value", [("importance", True), ("confidence", 2), ("needs_alert", "true"), ("title", "")]
)
def test_ai_schema_rejects_invalid(field, value):
    with pytest.raises(ValueError):
        validate_alert({**ALERT, field: value})


async def test_ai_cache_and_daily_budget(db, cfg):
    calls = []

    def handle(request):
        calls.append(request)
        return httpx.Response(
            200,
            json={
                "choices": [{"message": {"content": json.dumps(ALERT)}}],
                "usage": {"prompt_tokens": 50, "completion_tokens": 20},
            },
        )

    cfg.ai_daily_budget = 1
    client = AIClient(db, cfg, httpx.AsyncClient(transport=httpx.MockTransport(handle)))
    try:
        assert await client.complete("JSON", {"a": 1}, validate_alert) == ALERT
        await client.complete("JSON", {"a": 1}, validate_alert)
        assert len(calls) == 1
        with pytest.raises(AIError, match="daily_budget"):
            await client.complete("JSON", {"a": 2}, validate_alert)
        assert (await db.fetchone("SELECT SUM(prompt_tokens) n FROM ai_usage"))["n"] == 50
    finally:
        await client.close()


async def test_ai_auth_failure_does_not_leak_key(db, cfg):
    client = AIClient(
        db,
        cfg,
        httpx.AsyncClient(transport=httpx.MockTransport(lambda r: httpx.Response(401, json={}))),
    )
    try:
        with pytest.raises(AIError, match="http_401"):
            await client.complete("JSON", {}, validate_alert)
    finally:
        await client.close()


async def test_bot_429_persisted_retry(db, cfg):
    bot = Bot(
        db,
        cfg,
        client=httpx.AsyncClient(
            transport=httpx.MockTransport(
                lambda r: httpx.Response(
                    429, json={"ok": False, "error_code": 429, "parameters": {"retry_after": 99}}
                )
            )
        ),
    )
    try:
        await db.notify("one", 42, "test")
        await bot.send_step()
        row = await db.fetchone("SELECT * FROM notification_outbox")
        assert row["status"] == "pending" and row["retry_count"] == 1
        import time

        assert row["next_retry_at"] >= time.time() + 95
        assert cfg.bot_token not in row["last_error"]
    finally:
        await bot.close()


async def test_bot_auth_inbox_offset_and_command_dedup(db, cfg):
    updates = [
        {
            "update_id": 1,
            "message": {
                "from": {"id": 999},
                "chat": {"id": 999, "type": "private"},
                "text": "/status",
            },
        },
        {
            "update_id": 2,
            "message": {
                "from": {"id": 42},
                "chat": {"id": 42, "type": "private"},
                "text": "/report",
                "date": 1794384000,
            },
        },
        {
            "update_id": 3,
            "message": {
                "from": {"id": 42},
                "chat": {"id": -1001, "type": "supergroup"},
                "text": "/search secret",
            },
        },
    ]
    bot = Bot(
        db,
        cfg,
        client=httpx.AsyncClient(
            transport=httpx.MockTransport(
                lambda r: httpx.Response(200, json={"ok": True, "result": updates})
            )
        ),
    )
    try:
        await bot.poll_step()
        assert await db.get_state("bot_offset") == "4"
        assert (await db.fetchone("SELECT COUNT(*) n FROM bot_inbox"))["n"] == 1
        await bot.command_step()
        await bot.poll_step()
        assert not await bot.command_step()
        assert (await db.fetchone("SELECT COUNT(*) n FROM summaries"))["n"] == 1
        assert (await db.fetchone("SELECT COUNT(*) n FROM notification_outbox"))["n"] == 1
    finally:
        await bot.close()


async def test_search_literal_sql_wildcards_and_disabled_groups(db, cfg):
    await db.save_messages(
        [message(1, "100%优惠"), message(2, "100元优惠"), message(3, "100% hidden", chat=-999)]
    )
    result = await search(db, cfg, "100%")
    assert "100%优惠" in result and "100元优惠" not in result and "hidden" not in result


async def test_bot_permanent_error_does_not_block_queue(db, cfg):
    bot = Bot(
        db,
        cfg,
        client=httpx.AsyncClient(
            transport=httpx.MockTransport(
                lambda r: httpx.Response(403, json={"ok": False, "error_code": 403})
            )
        ),
    )
    try:
        await db.notify("x", 42, "test")
        await bot.send_step()
        assert (await db.fetchone("SELECT status FROM notification_outbox"))["status"] == "dead"
    finally:
        await bot.close()


async def test_slow_ai_does_not_block_message_writer(db, cfg):
    import asyncio

    from app.database import Writer

    entered, release = asyncio.Event(), asyncio.Event()

    async def slow(request):
        entered.set()
        await release.wait()
        return httpx.Response(200, json={"choices": [{"message": {"content": json.dumps(ALERT)}}]})

    ai = AIClient(db, cfg, httpx.AsyncClient(transport=httpx.MockTransport(slow)))
    writer = Writer(db, interval=0.01)
    writing = asyncio.create_task(writer.run())
    request = asyncio.create_task(ai.complete("JSON", {}, validate_alert))
    try:
        await asyncio.wait_for(entered.wait(), 1)
        await asyncio.wait_for(writer.submit(message(), wait=True), 1)
        assert not request.done()
        assert await db.fetchone("SELECT * FROM messages")
        release.set()
        await request
    finally:
        release.set()
        writing.cancel()
        request.cancel()
        await asyncio.gather(writing, request, return_exceptions=True)
        await ai.close()


async def test_ai_input_limit_before_network(db, cfg):
    ai = AIClient(
        db,
        cfg,
        httpx.AsyncClient(
            transport=httpx.MockTransport(
                lambda r: (_ for _ in ()).throw(AssertionError("network forbidden"))
            )
        ),
    )
    try:
        with pytest.raises(AIError, match="input_budget"):
            await ai.complete("JSON", {"text": "x" * 10000}, validate_alert)
        assert not await db.fetchone("SELECT * FROM ai_usage")
    finally:
        await ai.close()
