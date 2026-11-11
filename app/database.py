import asyncio
import os
import time
from contextlib import asynccontextmanager
from pathlib import Path

import aiosqlite

from .models import utcnow


class Database:
    def __init__(self, path):
        self.path = path
        self.lock = asyncio.Lock()
        self.conn = None

    async def open(self):
        Path(self.path).parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.conn = await aiosqlite.connect(self.path, isolation_level=None)
        self.conn.row_factory = aiosqlite.Row
        await self.conn.execute("PRAGMA journal_mode=WAL")
        await self.conn.execute("PRAGMA synchronous=NORMAL")
        await self.conn.execute("PRAGMA busy_timeout=5000")
        async with self.conn.execute("PRAGMA user_version") as cur:
            if (await cur.fetchone())[0] > 1:
                await self.close()
                raise ValueError("数据库版本高于当前程序，请勿直接降级")
        await self.conn.executescript(Path(__file__).with_name("schema.sql").read_text())
        os.chmod(self.path, 0o600)
        await self.execute("UPDATE alert_events SET status='pending' WHERE status='processing'")
        return self

    async def close(self):
        if self.conn:
            await self.conn.close()

    @asynccontextmanager
    async def transaction(self):
        async with self.lock:
            await self.conn.execute("BEGIN IMMEDIATE")
            try:
                yield self.conn
                await self.conn.commit()
            except BaseException:
                await self.conn.rollback()
                raise

    async def fetchall(self, sql, args=()):
        async with self.lock, self.conn.execute(sql, args) as cur:
            return [dict(r) for r in await cur.fetchall()]

    async def fetchone(self, sql, args=()):
        rows = await self.fetchall(sql, args)
        return rows[0] if rows else None

    async def execute(self, sql, args=()):
        async with self.transaction() as conn:
            cur = await conn.execute(sql, args)
            return cur.lastrowid

    async def get_state(self, key, default=None):
        row = await self.fetchone("SELECT value FROM state WHERE key=?", (key,))
        return row["value"] if row else default

    async def set_state(self, key, value):
        await self.execute(
            "INSERT INTO state VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, str(value)),
        )

    async def save_messages(self, messages):
        async with self.transaction() as conn:
            for m in messages:
                cur = await conn.execute(
                    "SELECT 1 FROM deletions WHERE chat_id=? AND message_id=?",
                    (m.chat_id, m.message_id),
                )
                if await cur.fetchone():
                    continue
                await conn.execute(
                    """
                INSERT INTO messages VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(chat_id,message_id) DO UPDATE SET
                  sender_id=excluded.sender_id, message_text=excluded.message_text,
                  edited_at=excluded.edited_at, reply_to_id=excluded.reply_to_id,
                  has_media=excluded.has_media, message_link=excluded.message_link,
                  deleted=excluded.deleted, revision=excluded.revision,
                  analysis_pending=excluded.analysis_pending
                WHERE messages.deleted=0 AND excluded.revision!=messages.revision
                  AND COALESCE(excluded.edited_at,excluded.message_date)>=COALESCE(messages.edited_at,messages.message_date)
                """,
                    (
                        m.chat_id,
                        m.message_id,
                        m.sender_id,
                        m.message_text,
                        m.message_date,
                        utcnow(),
                        m.edited_at,
                        m.reply_to_id,
                        int(m.has_media),
                        m.message_link,
                        int(m.deleted),
                        m.revision,
                        int(m.analyze and bool(m.message_text)),
                    ),
                )

    async def checkpoint(self, chat_id, message_id):
        # ONLY the ordered history scan calls this; live ingestion never advances it.
        await self.execute(
            """INSERT INTO sync_state VALUES(?,?,?,NULL)
            ON CONFLICT(chat_id) DO UPDATE SET
            last_message_id=MAX(last_message_id,excluded.last_message_id),
            last_sync_at=excluded.last_sync_at,last_error=NULL""",
            (chat_id, message_id, utcnow()),
        )

    async def mark_deleted(self, chat_id, ids):
        async with self.transaction() as conn:
            for mid in ids:
                await conn.execute(
                    "INSERT OR IGNORE INTO deletions VALUES(?,?,?)", (chat_id, mid, utcnow())
                )
                await conn.execute(
                    "UPDATE messages SET deleted=1,analysis_pending=0 WHERE chat_id=? AND message_id=?",
                    (chat_id, mid),
                )

    @staticmethod
    async def enqueue(conn, key, destination, text):
        # Plain text, conservative UTF-16 limit; emoji count as two code units.
        chunks, chunk, units = [], "", 0
        for char in text:
            size = len(char.encode("utf-16-le")) // 2
            if units + size > 3500:
                chunks.append(chunk)
                chunk, units = "", 0
            chunk += char
            units += size
        if chunk:
            chunks.append(chunk)
        for i, part in enumerate(chunks):
            await conn.execute(
                """INSERT OR IGNORE INTO notification_outbox
                (dedupe_key,chat_id,message_text,created_at) VALUES(?,?,?,?)""",
                (f"{key}:{i}", destination, part, utcnow()),
            )

    async def notify(self, key, destination, text):
        async with self.transaction() as conn:
            await self.enqueue(conn, key, destination, text)

    async def queue_report(self, key, destination, start, end):
        await self.execute(
            """INSERT OR IGNORE INTO summaries
            (task_key,destination,period_start,period_end,created_at) VALUES(?,?,?,?,?)""",
            (key, destination, start, end, utcnow()),
        )

    async def backup(self, path):
        Path(path).parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        # The backup API cooperates with WAL; never copy only the live .db file.
        async with self.lock, aiosqlite.connect(str(path) + ".tmp") as target:
            await self.conn.backup(target, pages=128, sleep=0.05)
        os.chmod(str(path) + ".tmp", 0o600)
        os.replace(str(path) + ".tmp", path)


class Writer:
    def __init__(self, db, queue_size=2000, batch_size=100, interval=1):
        self.db = db
        self.queue = asyncio.Queue(maxsize=queue_size)
        self.batch_size, self.interval = batch_size, interval

    async def submit(self, message, wait=False):
        ack = asyncio.get_running_loop().create_future() if wait else None
        await self.queue.put((message, ack))
        if ack:
            await ack

    async def run(self):
        while True:
            batch = [await self.queue.get()]
            deadline = time.monotonic() + self.interval
            while len(batch) < self.batch_size:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                try:
                    batch.append(await asyncio.wait_for(self.queue.get(), remaining))
                except TimeoutError:
                    break
            try:
                for attempt in range(4):
                    try:
                        await self.db.save_messages([m for m, _ in batch])
                        break
                    except aiosqlite.OperationalError:
                        if attempt == 3:
                            raise
                        await asyncio.sleep(2**attempt)
            except BaseException as exc:
                for _, ack in batch:
                    if ack and not ack.done():
                        ack.set_exception(exc)
                raise  # fail fast: supervisor restarts; durable history cursor is unchanged
            else:
                for _, ack in batch:
                    if ack and not ack.done():
                        ack.set_result(None)
            finally:
                for _ in batch:
                    self.queue.task_done()
