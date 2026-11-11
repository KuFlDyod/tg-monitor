import asyncio
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

from .models import stamp


class Scheduler:
    def __init__(self, db, cfg):
        self.db, self.cfg = db, cfg

    async def step(self, now=None):
        now = now or datetime.now(UTC)
        local = now.astimezone(self.cfg.zone)
        due = local.replace(
            hour=self.cfg.report_hour, minute=self.cfg.report_minute, second=0, microsecond=0
        )
        before_today = local < due
        if before_today:
            due -= timedelta(days=1)
        last = await self.db.get_state("report_schedule_end")
        if last is None and before_today:
            await self.db.set_state("report_schedule_end", stamp(due))
            return
        start = (
            datetime.fromisoformat(last).astimezone(self.cfg.zone)
            if last
            else due - timedelta(days=1)
        )
        # One window per tick. Restart catches up without swallowing failed windows.
        end = start + timedelta(days=1)
        if end <= due:
            async with self.db.transaction() as conn:
                await conn.execute(
                    """INSERT OR IGNORE INTO summaries(task_key,destination,period_start,period_end,created_at)
                    VALUES(?,?,?,?,?)""",
                    (
                        "daily:" + stamp(end),
                        self.cfg.bot_chat_id,
                        stamp(start),
                        stamp(end),
                        stamp(now),
                    ),
                )
                await conn.execute(
                    "INSERT INTO state VALUES('report_schedule_end',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                    (stamp(end),),
                )

    async def maintenance(self):
        now = datetime.now(UTC)
        day = now.date().isoformat()
        if await self.db.get_state("maintenance_day") == day:
            return
        backup_dir = Path(self.cfg.database_path).parent / "backups"
        await self.db.backup(backup_dir / f"telegram-{day}.db")
        cutoff = stamp(now - timedelta(days=self.cfg.retention_days))
        cache_cutoff = stamp(now - timedelta(days=self.cfg.cache_days))
        # Do not delete messages needed by a pending report or candidate analysis.
        pending = await self.db.fetchone(
            "SELECT MIN(period_start) AS start FROM summaries WHERE status='pending'"
        )
        if pending and pending["start"]:
            cutoff = min(cutoff, pending["start"])
        # Small transactions keep ingestion latency bounded.
        for _ in range(20):
            async with self.db.transaction() as conn:
                cur = await conn.execute(
                    "DELETE FROM messages WHERE rowid IN (SELECT rowid FROM messages WHERE collected_at<? AND analysis_pending=0 LIMIT 1000)",
                    (cutoff,),
                )
            if cur.rowcount < 1000:
                break
            await asyncio.sleep(0)
        async with self.db.transaction() as conn:
            await conn.execute("DELETE FROM ai_cache WHERE created_at<?", (cache_cutoff,))
            await conn.execute("DELETE FROM ai_usage WHERE created_at<?", (cutoff,))
            await conn.execute("DELETE FROM alert_dedupe WHERE expires_at<?", (time.time(),))
            await conn.execute(
                "DELETE FROM alert_events WHERE created_at<? AND status NOT IN ('pending','processing')",
                ((now - timedelta(days=self.cfg.retention_days)).timestamp(),),
            )
            await conn.execute(
                "DELETE FROM notification_outbox WHERE created_at<? AND status='sent'", (cutoff,)
            )
            await conn.execute(
                "DELETE FROM summary_parts WHERE job_id IN (SELECT id FROM summaries WHERE status!='pending' AND created_at<?)",
                (cache_cutoff,),
            )
            await conn.execute(
                "DELETE FROM summary_inputs WHERE job_id IN (SELECT id FROM summaries WHERE status!='pending' AND created_at<?)",
                (cache_cutoff,),
            )
            await conn.execute("DELETE FROM deletions WHERE observed_at<?", (cutoff,))
            await conn.execute(
                "DELETE FROM bot_inbox WHERE status='done' AND update_id < (SELECT COALESCE(MAX(update_id),0)-1000 FROM bot_inbox)"
            )
        for file in backup_dir.glob("telegram-*.db"):
            if file.stat().st_mtime < time.time() - self.cfg.backup_days * 86400:
                file.unlink()
        await self.db.set_state("maintenance_day", day)

    async def run(self):
        while True:
            await self.step()
            await self.maintenance()
            await asyncio.sleep(30)
