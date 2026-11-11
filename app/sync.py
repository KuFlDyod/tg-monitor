import asyncio
import logging

from telethon.errors import FloodWaitError

from .collector import convert

log = logging.getLogger(__name__)


class Sync:
    def __init__(self, client, db, writer, cfg, entities):
        self.client, self.db, self.writer, self.cfg, self.entities = (
            client,
            db,
            writer,
            cfg,
            entities,
        )

    async def group(self, chat_id, entity):
        state = await self.db.fetchone("SELECT * FROM sync_state WHERE chat_id=?", (chat_id,))
        cursor = state["last_message_id"] if state else 0
        latest = await self.client.get_messages(entity, limit=1)
        if not latest:
            return
        head = latest[0].id
        if cursor == 0:
            # Descending, paginated iterator, bounded batches. A partial initial scan never
            # advances the cursor. Repeating it after a crash is safe via the primary key.
            batch = []
            async for msg in self.client.iter_messages(
                entity, limit=self.cfg.history_limit, max_id=head + 1, wait_time=1
            ):
                batch.append(convert(msg, chat_id, entity, analyze=False))
                if len(batch) >= self.cfg.db_batch_size:
                    await self.db.save_messages(batch)
                    batch.clear()
            if batch:
                await self.db.save_messages(batch)
            await self.db.checkpoint(chat_id, head)
        elif head > cursor:
            batch = []
            async for msg in self.client.iter_messages(
                entity, min_id=cursor, max_id=head + 1, reverse=True, limit=None, wait_time=1
            ):
                batch.append(convert(msg, chat_id, entity))
                if len(batch) >= self.cfg.db_batch_size:
                    await self.db.save_messages(batch)
                    # Committed strictly ascending range, not MAX(messages.message_id).
                    await self.db.checkpoint(chat_id, batch[-1].message_id)
                    batch.clear()
            if batch:
                await self.db.save_messages(batch)
                await self.db.checkpoint(chat_id, batch[-1].message_id)
            # Numeric gaps can be deleted/hidden messages, not necessarily missed messages.
            await self.db.checkpoint(chat_id, head)
        else:
            await self.db.checkpoint(chat_id, cursor)

    async def run(self):
        while True:
            for cid, entity in self.entities.items():
                try:
                    await self.group(cid, entity)
                except FloodWaitError as exc:
                    await self.db.execute(
                        "INSERT INTO sync_state(chat_id,last_error) VALUES(?,?) ON CONFLICT(chat_id) DO UPDATE SET last_error=excluded.last_error",
                        (cid, f"flood_wait:{exc.seconds}"),
                    )
                    await asyncio.sleep(exc.seconds + 1)
                except Exception as exc:
                    # Preserve the last committed cursor on any partial scan failure.
                    await self.db.execute(
                        "INSERT INTO sync_state(chat_id,last_error) VALUES(?,?) ON CONFLICT(chat_id) DO UPDATE SET last_error=excluded.last_error",
                        (cid, type(exc).__name__),
                    )
                    log.warning("补采未完成 chat=%s error=%s", cid, type(exc).__name__)
            await asyncio.sleep(self.cfg.sync_interval)
