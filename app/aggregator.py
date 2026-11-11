import asyncio
import json
import time

from .models import digest
from .rules import event_key, matches


class Aggregator:
    def __init__(self, db, cfg):
        self.db, self.cfg = db, cfg
        self.enabled = {g.id for g in cfg.active_groups if g.alert_enabled}

    async def step(self):
        rows = await self.db.fetchall(
            "SELECT * FROM messages WHERE analysis_pending=1 ORDER BY collected_at LIMIT 100"
        )
        for row in rows:
            now = time.time()
            rules = [
                r
                for r in self.cfg.rules
                if row["chat_id"] in self.enabled
                and not row["deleted"]
                and matches(r, row["chat_id"], row["message_text"])
            ]
            async with self.db.transaction() as conn:
                for rule in rules:
                    fingerprint = digest([row["chat_id"], rule.id, event_key(row["message_text"])])
                    cur = await conn.execute(
                        "SELECT 1 FROM alert_dedupe WHERE key=? AND expires_at>?",
                        (fingerprint, now),
                    )
                    if await cur.fetchone():
                        continue
                    cur = await conn.execute(
                        """SELECT * FROM alert_events WHERE chat_id=? AND rule_id=?
                        AND status='pending' AND ready_at>? ORDER BY id DESC LIMIT 1""",
                        (row["chat_id"], rule.id, now),
                    )
                    pending = await cur.fetchone()
                    payload = json.loads(pending["payload"]) if pending else []
                    item = {
                        "id": f"{row['chat_id']}:{row['message_id']}",
                        "text": row["message_text"],
                        "date": row["message_date"],
                        "link": row["message_link"],
                    }
                    # Bounded event payload; split into new events rather than drop candidates.
                    if pending and (
                        len(payload) >= 30
                        or len(json.dumps(payload, ensure_ascii=False).encode())
                        + len(item["text"].encode())
                        > self.cfg.ai_input_tokens - 1600
                    ):
                        pending, payload = None, []
                    payload.append(item)
                    raw = json.dumps(payload, ensure_ascii=False)
                    if pending:
                        await conn.execute(
                            "UPDATE alert_events SET payload=?,updated_at=? WHERE id=?",
                            (raw, now, pending["id"]),
                        )
                    else:
                        await conn.execute(
                            """INSERT INTO alert_events
                            (chat_id,rule_id,event_key,payload,created_at,ready_at,updated_at)
                            VALUES(?,?,?,?,?,?,?)""",
                            (
                                row["chat_id"],
                                rule.id,
                                fingerprint,
                                raw,
                                now,
                                now + rule.aggregation_seconds,
                                now,
                            ),
                        )
                    await conn.execute(
                        "INSERT INTO alert_dedupe VALUES(?,?) ON CONFLICT(key) DO UPDATE SET expires_at=excluded.expires_at",
                        (
                            fingerprint,
                            now + max(rule.cooldown_seconds, rule.aggregation_seconds, 1),
                        ),
                    )
                # Revision guard: an edit arriving during evaluation remains pending.
                await conn.execute(
                    "UPDATE messages SET analysis_pending=0 WHERE chat_id=? AND message_id=? AND revision=?",
                    (row["chat_id"], row["message_id"], row["revision"]),
                )
        return len(rows)

    async def run(self):
        while True:
            count = await self.step()
            await asyncio.sleep(0 if count else 1)
