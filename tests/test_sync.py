from types import SimpleNamespace

import pytest
from conftest import message, telegram_message

from app.collector import convert
from app.sync import Sync


class FakeTelegram:
    def __init__(self, ids, fail_after=None):
        self.ids, self.fail_after = ids, fail_after

    async def get_messages(self, entity, limit):
        return [telegram_message(max(self.ids))] if self.ids else []

    async def iter_messages(
        self, entity, limit=None, min_id=0, max_id=10**10, reverse=False, **kwargs
    ):
        ids = sorted([i for i in self.ids if min_id < i < max_id], reverse=not reverse)
        for index, mid in enumerate(ids[:limit] if limit else ids):
            if index == self.fail_after:
                raise ConnectionError("interrupted")
            yield telegram_message(mid)


async def test_live_high_id_does_not_skip_missing_history(db, cfg):
    await db.save_messages([message(1)])
    await db.checkpoint(-1000000000123, 1)
    await db.save_messages([message(100)])
    sync = Sync(FakeTelegram([1, 2, 3, 100]), db, None, cfg, {})
    await sync.group(-1000000000123, SimpleNamespace())
    assert [
        r["message_id"]
        for r in await db.fetchall("SELECT message_id FROM messages ORDER BY message_id")
    ] == [1, 2, 3, 100]
    assert (await db.fetchone("SELECT * FROM sync_state"))["last_message_id"] == 100


async def test_initial_partial_scan_keeps_zero_cursor(db, cfg):
    cfg.db_batch_size = 2
    sync = Sync(FakeTelegram([1, 2, 3, 4], fail_after=2), db, None, cfg, {})
    with pytest.raises(ConnectionError):
        await sync.group(-1000000000123, SimpleNamespace())
    assert await db.fetchone("SELECT * FROM sync_state") is None
    sync.client = FakeTelegram([1, 2, 3, 4])
    await sync.group(-1000000000123, SimpleNamespace())
    assert (await db.fetchone("SELECT COUNT(*) n FROM messages"))["n"] == 4
    assert (await db.fetchone("SELECT * FROM sync_state"))["last_message_id"] == 4


async def test_ordered_partial_sync_only_checkpoints_committed_prefix(db, cfg):
    cfg.db_batch_size = 2
    await db.checkpoint(-1000000000123, 1)
    sync = Sync(FakeTelegram([2, 3, 4, 5], fail_after=3), db, None, cfg, {})
    with pytest.raises(ConnectionError):
        await sync.group(-1000000000123, SimpleNamespace())
    assert (await db.fetchone("SELECT * FROM sync_state"))["last_message_id"] == 3
    assert [
        r["message_id"] for r in await db.fetchall("SELECT * FROM messages ORDER BY message_id")
    ] == [2, 3]


def test_links_only_when_supported():
    assert convert(telegram_message(), -23).message_link is None
    assert convert(telegram_message(), -100123).message_link is None
    assert convert(telegram_message(), -1000000000123).message_link == "https://t.me/c/123/1"
    assert (
        convert(telegram_message(), -1000000000123, SimpleNamespace(username="public")).message_link
        == "https://t.me/public/1"
    )
