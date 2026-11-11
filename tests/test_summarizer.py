from datetime import UTC, datetime, timedelta

import pytest
from conftest import message

from app.ai_client import AIError
from app.models import stamp
from app.scheduler import Scheduler
from app.summarizer import Summarizer


class FakeAI:
    def __init__(self):
        self.calls = []
        self.seen = set()

    async def complete(self, system, data, validator):
        self.calls.append(data)
        refs = []
        for item in data["items"]:
            refs.extend(item.get("refs", [item.get("id")]))
            if "id" in item:
                self.seen.add(item["id"])
        return validator(
            {
                "text": "已汇总所有输入，优惠、故障信息待核实。",
                "refs": list(dict.fromkeys(refs))[:6],
            }
        )


async def queue(db):
    now = datetime.now(UTC)
    await db.queue_report(
        "daily:test", 42, stamp(now - timedelta(days=1)), stamp(now + timedelta(minutes=1))
    )


async def test_all_messages_including_last_chunk_are_processed(db, cfg):
    await db.save_messages([message(i, f"消息 {i} " + ("技术讨论" * 60)) for i in range(1, 121)])
    await queue(db)
    ai = FakeAI()
    summary = Summarizer(db, cfg, ai)
    await summary.step()
    assert len(ai.seen) == 120
    assert "-1000000000123:120" in ai.seen
    assert len(ai.calls) > 2
    assert (await db.fetchone("SELECT status FROM summaries"))["status"] == "done"
    assert (
        "120 条消息"
        in (await db.fetchone("SELECT message_text FROM notification_outbox"))["message_text"]
    )


async def test_report_failure_reuses_immutable_snapshot(db, cfg):
    await db.save_messages([message(i, "优惠 " + str(i) + " 技术" * 300) for i in range(1, 10)])
    await queue(db)

    class Flaky(FakeAI):
        failed = False

        async def complete(self, *args):
            if len(self.calls) == 1 and not self.failed:
                self.failed = True
                raise AIError("temporary", 1)
            return await super().complete(*args)

    ai = Flaky()
    summary = Summarizer(db, cfg, ai)
    await summary.step()
    assert (await db.fetchone("SELECT status FROM summaries"))["status"] == "pending"
    first_calls = len(ai.calls)
    assert first_calls == 1
    await db.save_messages([message(99, "新增不应混入已冻结报告")])
    await db.execute("UPDATE summaries SET next_retry_at=0")
    await summary.step()
    assert (await db.fetchone("SELECT status FROM summaries"))["status"] == "done"
    assert "-1000000000123:99" not in ai.seen
    assert (await db.fetchone("SELECT COUNT(*) n FROM summary_inputs"))["n"] == 9


async def test_hallucinated_citation_rejected(db, cfg):
    class BadAI:
        async def complete(self, system, data, validator):
            with pytest.raises(ValueError, match="citations"):
                validator({"text": "bad", "refs": ["-999:999"]})
            raise AIError("bad_citations")

    await db.save_messages([message()])
    await queue(db)
    await Summarizer(db, cfg, BadAI()).step()
    assert not await db.fetchone("SELECT * FROM notification_outbox")


async def test_scheduler_timezone_restart_and_failed_window(db, cfg):
    s = Scheduler(db, cfg)
    # 21:00 Shanghai = 13:00 UTC
    now = datetime(2026, 11, 11, 13, 0, tzinfo=UTC)
    await s.step(now)
    await s.step(now)
    assert (await db.fetchone("SELECT COUNT(*) n FROM summaries"))["n"] == 1
    first = await db.fetchone("SELECT * FROM summaries")
    assert first["period_end"] == stamp(now)
    await s.step(now + timedelta(days=1))
    rows = await db.fetchall("SELECT * FROM summaries ORDER BY id")
    assert len(rows) == 2 and rows[1]["period_start"] == rows[0]["period_end"]
    assert all(r["status"] == "pending" for r in rows)


async def test_ai_disabled_does_not_retry_forever(db, cfg):
    cfg.ai_enabled = False
    await queue(db)
    await Summarizer(db, cfg, None).step()
    assert (await db.fetchone("SELECT status FROM summaries"))["status"] == "disabled"


async def test_first_start_before_schedule_waits_until_due(db, cfg):
    scheduler = Scheduler(db, cfg)
    now = datetime(2026, 11, 11, 12, 0, tzinfo=UTC)
    await scheduler.step(now)
    assert not await db.fetchone("SELECT * FROM summaries")
    await scheduler.step(now + timedelta(hours=1))
    assert (await db.fetchone("SELECT period_end FROM summaries"))["period_end"] == stamp(
        now + timedelta(hours=1)
    )
