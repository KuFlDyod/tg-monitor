import asyncio
import sqlite3
from dataclasses import replace

import pytest
from conftest import message

from app.database import Writer


async def test_dedup_does_not_advance_sync_cursor(db):
    m = message()
    await db.save_messages([m, m])
    assert (await db.fetchone("SELECT COUNT(*) n FROM messages"))["n"] == 1
    assert await db.fetchone("SELECT * FROM sync_state") is None


async def test_edit_requeues_but_stale_history_cannot_overwrite(db):
    m = message()
    await db.save_messages([m])
    await db.execute("UPDATE messages SET analysis_pending=0")
    edited = replace(m, message_text="edited", edited_at="2099-01-01T00:00:00.000000+00:00")
    await db.save_messages([edited, m])
    row = await db.fetchone("SELECT * FROM messages")
    assert row["message_text"] == "edited"
    assert row["analysis_pending"] == 1


async def test_delete_before_writer_cannot_resurrect(db):
    await db.mark_deleted(-1000000000123, [1])
    await db.save_messages([message()])
    assert not await db.fetchone("SELECT * FROM messages")


async def test_batch_is_atomic(db):
    with pytest.raises(Exception):
        await db.save_messages([message(), replace(message(2), message_date=None)])
    assert not await db.fetchone("SELECT * FROM messages")


async def test_writer_ack_after_commit(db):
    writer = Writer(db, queue_size=2, batch_size=2, interval=0.01)
    worker = asyncio.create_task(writer.run())
    try:
        await writer.submit(message(), wait=True)
        assert await db.fetchone("SELECT * FROM messages")
        await writer.queue.join()
    finally:
        worker.cancel()
        await asyncio.gather(worker, return_exceptions=True)


async def test_transaction_rollback_includes_outbox(db):
    with pytest.raises(RuntimeError):
        async with db.transaction() as conn:
            await db.enqueue(conn, "a", 42, "hello")
            raise RuntimeError("crash")
    assert not await db.fetchone("SELECT * FROM notification_outbox")


async def test_outbox_idempotency_and_unicode_limit(db):
    text = "😀" * 5000
    await db.notify("same", 42, text)
    await db.notify("same", 42, text)
    rows = await db.fetchall("SELECT * FROM notification_outbox ORDER BY id")
    assert "".join(r["message_text"] for r in rows) == text
    assert all(len(r["message_text"].encode("utf-16-le")) // 2 <= 3500 for r in rows)


async def test_backup_is_consistent(db, tmp_path):
    await db.save_messages([message()])
    path = tmp_path / "backup.db"
    await db.backup(path)
    with sqlite3.connect(path) as conn:
        assert conn.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        assert conn.execute("SELECT COUNT(*) FROM messages").fetchone()[0] == 1


async def test_reopen_recovers_processing_event(tmp_path):
    from app.database import Database

    path = str(tmp_path / "recovery.db")
    one = await Database(path).open()
    await one.execute(
        "INSERT INTO alert_events(chat_id,rule_id,payload,status,created_at,ready_at,updated_at) VALUES(-1,'r','[]','processing',0,0,0)"
    )
    await one.close()
    two = await Database(path).open()
    try:
        assert (await two.fetchone("SELECT status FROM alert_events"))["status"] == "pending"
    finally:
        await two.close()


async def test_writer_failure_keeps_cursor_and_propagates(db, monkeypatch):
    writer = Writer(db, interval=0.01)

    async def fail(messages):
        raise RuntimeError("disk failure")

    monkeypatch.setattr(db, "save_messages", fail)
    task = asyncio.create_task(writer.run())
    with pytest.raises(RuntimeError, match="disk failure"):
        await writer.submit(message(), wait=True)
    with pytest.raises(RuntimeError, match="disk failure"):
        await task
    assert not await db.fetchone("SELECT * FROM sync_state")
