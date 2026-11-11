from conftest import message

from app.aggregator import Aggregator
from app.ai_client import AIError
from app.alerts import Alerts


async def test_local_event_idempotent(db, cfg):
    await db.save_messages([message()])
    agg = Aggregator(db, cfg)
    await agg.step()
    await agg.step()
    alert = Alerts(db, cfg, None)
    assert await alert.step()
    assert not await alert.step()
    assert (await db.fetchone("SELECT COUNT(*) n FROM notification_outbox"))["n"] == 1


async def test_duplicate_text_and_cooldown(db, cfg):
    agg, alert = Aggregator(db, cfg), Alerts(db, cfg, None)
    await db.save_messages([message()])
    await agg.step()
    await alert.step()
    await db.save_messages([message(2), message(3, "优惠 new")])
    await agg.step()
    await alert.step()
    assert (await db.fetchone("SELECT COUNT(*) n FROM notification_outbox"))["n"] == 1
    assert (await db.fetchone("SELECT COUNT(*) n FROM alert_events WHERE status='suppressed'"))[
        "n"
    ] == 1


async def test_window_aggregates_messages(db, cfg):
    cfg.rules[0].aggregation_seconds = 20
    await db.save_messages([message(1, "优惠 A"), message(2, "优惠 B")])
    await Aggregator(db, cfg).step()
    rows = await db.fetchall("SELECT * FROM alert_events")
    assert len(rows) == 1
    import json

    assert len(json.loads(rows[0]["payload"])) == 2


async def test_ai_error_persists_retry_and_collection_continues(db, cfg):
    cfg.rules[0].ai_check = True

    class Failed:
        async def complete(self, *args):
            raise AIError("http_503", 60)

    await db.save_messages([message()])
    await Aggregator(db, cfg).step()
    await Alerts(db, cfg, Failed()).step()
    event = await db.fetchone("SELECT * FROM alert_events")
    assert event["status"] == "pending" and event["retry_count"] == 1
    await db.save_messages([message(2)])
    assert (await db.fetchone("SELECT COUNT(*) n FROM messages"))["n"] == 2


async def test_deleted_candidate_not_sent(db, cfg):
    await db.save_messages([message()])
    await Aggregator(db, cfg).step()
    await db.mark_deleted(-1000000000123, [1])
    await Alerts(db, cfg, None).step()
    assert not await db.fetchone("SELECT * FROM notification_outbox")
