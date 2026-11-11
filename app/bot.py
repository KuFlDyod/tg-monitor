import asyncio
import json
import time
from datetime import UTC, datetime, timedelta

import httpx

from .metrics import status
from .models import stamp
from .search import search

HELP = """Telegram AI 监控助手（仅限授权私聊）
/start /help — 帮助
/status — 连接、队列、AI 用量
/groups — 监控群组
/search 关键词 — 搜索历史
/report — 最近 24 小时汇总
仅采集账号有访问权限的群组；AI 输出需核实。"""


class BotError(Exception):
    def __init__(self, code, retry_after=30):
        super().__init__(code)
        self.retry_after = retry_after


class Bot:
    def __init__(self, db, cfg, writer=None, collector=None, client=None):
        self.db, self.cfg, self.writer, self.collector = db, cfg, writer, collector
        self.client = client or httpx.AsyncClient(timeout=40, proxy=cfg.http_proxy or None)

    async def close(self):
        await self.client.aclose()

    async def request(self, method, payload):
        try:
            response = await self.client.post(
                f"https://api.telegram.org/bot{self.cfg.bot_token}/{method}", json=payload
            )
            data = response.json()
            if response.status_code >= 400 or not data.get("ok"):
                retry = float(data.get("parameters", {}).get("retry_after", 30))
                raise BotError(
                    f"bot_{data.get('error_code', response.status_code)}", max(1, min(retry, 86400))
                )
            return data["result"]
        except (httpx.HTTPError, ValueError, KeyError, TypeError, AttributeError):
            # Never propagate httpx exception strings: URLs contain the bot token.
            raise BotError("bot_transport_or_invalid_response") from None

    async def send_step(self):
        # Strict ID ordering avoids splitting a report across unrelated notifications.
        row = await self.db.fetchone(
            "SELECT * FROM notification_outbox WHERE status='pending' ORDER BY id LIMIT 1"
        )
        if not row or row["next_retry_at"] > time.time():
            return False
        try:
            await self.request(
                "sendMessage",
                {
                    "chat_id": row["chat_id"],
                    "text": row["message_text"],
                    "link_preview_options": {"is_disabled": True},
                },
            )
            await self.db.execute(
                "UPDATE notification_outbox SET status='sent',sent_at=?,last_error=NULL WHERE id=?",
                (stamp(datetime.now(UTC)), row["id"]),
            )
            await self.db.set_state("bot_last_error", "无")
        except BotError as exc:
            if str(exc) in ("bot_400", "bot_403"):
                await self.db.execute(
                    "UPDATE notification_outbox SET status='dead',last_error=? WHERE id=?",
                    (str(exc), row["id"]),
                )
                await self.db.set_state("bot_last_error", str(exc) + ":dead_letter")
                return True
            delay = max(exc.retry_after, min(3600, 2 ** min(row["retry_count"] + 1, 12)))
            await self.db.execute(
                "UPDATE notification_outbox SET retry_count=retry_count+1,next_retry_at=?,last_error=? WHERE id=?",
                (time.time() + delay, str(exc), row["id"]),
            )
            await self.db.set_state("bot_last_error", str(exc))
        return True

    async def sender(self):
        while True:
            await self.send_step()
            await asyncio.sleep(1.1)

    async def poll_step(self):
        offset = int(await self.db.get_state("bot_offset", "0"))
        updates = await self.request(
            "getUpdates",
            {"offset": offset, "timeout": 25, "limit": 100, "allowed_updates": ["message"]},
        )
        if not isinstance(updates, list):
            raise BotError("invalid_updates")
        if updates:
            async with self.db.transaction() as conn:
                for update in updates:
                    # Drop unauthorized updates before persisting personal data.
                    m = update.get("message", {})
                    if self.authorized(m):
                        await conn.execute(
                            "INSERT OR IGNORE INTO bot_inbox(update_id,payload) VALUES(?,?)",
                            (update["update_id"], json.dumps(update, ensure_ascii=False)),
                        )
                highest = max(u["update_id"] for u in updates) + 1
                await conn.execute(
                    "INSERT INTO state VALUES('bot_offset',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                    (str(highest),),
                )

    def authorized(self, message):
        user = message.get("from", {}).get("id")
        chat = message.get("chat", {})
        return (
            user in self.cfg.allowed_user_ids
            and chat.get("type") == "private"
            and chat.get("id") == user
        )

    async def poll(self):
        while True:
            # Bound persisted command backlog without acknowledging unseen updates.
            pending = await self.db.fetchone(
                "SELECT COUNT(*) AS n FROM bot_inbox WHERE status='pending'"
            )
            if pending["n"] > 1000:
                await asyncio.sleep(5)
                continue
            try:
                await self.poll_step()
            except BotError as exc:
                await self.db.set_state("bot_last_error", str(exc))
                await asyncio.sleep(max(5, exc.retry_after))

    async def command_step(self):
        row = await self.db.fetchone(
            "SELECT * FROM bot_inbox WHERE status='pending' ORDER BY update_id LIMIT 1"
        )
        if not row:
            return False
        update = json.loads(row["payload"])
        message = update.get("message", {})
        if not self.authorized(message):
            await self.db.execute(
                "UPDATE bot_inbox SET status='done' WHERE update_id=?", (row["update_id"],)
            )
            return True
        text = message.get("text", "").strip()
        command, _, arg = text.partition(" ")
        command = command.split("@")[0].lower()
        user = message["from"]["id"]
        job = None
        if command in ("/help", "/start"):
            reply = HELP
        elif command == "/groups":
            reply = "\n".join(
                f"{g.name}: {g.id} 汇总={g.summary_enabled} 告警={g.alert_enabled}"
                for g in self.cfg.active_groups
            )
        elif command == "/status":
            reply = await status(
                self.db,
                self.cfg,
                self.writer.queue.qsize() if self.writer else 0,
                self.collector.client.is_connected() if self.collector else False,
            )
        elif command == "/search":
            reply = await search(self.db, self.cfg, arg)
        elif command == "/report":
            # Stable time from the update, not retry time; one outstanding manual report per user.
            end = datetime.fromtimestamp(message.get("date", int(time.time())), UTC)
            pending = await self.db.fetchone(
                "SELECT id FROM summaries WHERE destination=? AND status='pending' AND task_key LIKE 'manual:%'",
                (user,),
            )
            if pending:
                reply = f"已有待处理汇总 #{pending['id']}，请勿重复提交。"
            else:
                job = (
                    f"manual:{row['update_id']}",
                    user,
                    stamp(end - timedelta(hours=24)),
                    stamp(end),
                    stamp(datetime.now(UTC)),
                )
                reply = "已排队生成最近 24 小时汇总，完成后私聊发送。"
        else:
            reply = "未知命令。\n" + HELP
        async with self.db.transaction() as conn:
            if job:
                await conn.execute(
                    "INSERT OR IGNORE INTO summaries(task_key,destination,period_start,period_end,created_at) VALUES(?,?,?,?,?)",
                    job,
                )
            await self.db.enqueue(conn, f"command:{row['update_id']}", user, reply)
            await conn.execute(
                "UPDATE bot_inbox SET status='done' WHERE update_id=?", (row["update_id"],)
            )
        return True

    async def commands(self):
        while True:
            found = await self.command_step()
            await asyncio.sleep(0 if found else 0.5)
