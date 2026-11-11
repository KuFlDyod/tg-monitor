import asyncio
import logging
from pathlib import Path
from urllib.parse import unquote, urlparse

from telethon import TelegramClient, events, utils
from telethon.tl.types import PeerChannel

from .models import Message, stamp

log = logging.getLogger(__name__)


def create_client(cfg):
    proxy = None
    if cfg.tg_proxy:
        from python_socks import ProxyType

        parsed = urlparse(cfg.tg_proxy)
        types = {"http": ProxyType.HTTP, "socks5": ProxyType.SOCKS5, "socks4": ProxyType.SOCKS4}
        if parsed.scheme not in types or not parsed.hostname or not parsed.port:
            raise ValueError("TG_PROXY 应为 http://host:port 或 socks5://host:port")
        proxy = (
            types[parsed.scheme],
            parsed.hostname,
            parsed.port,
            True,
            unquote(parsed.username) if parsed.username else None,
            unquote(parsed.password) if parsed.password else None,
        )
    Path(cfg.session_path).parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    return TelegramClient(
        cfg.session_path,
        cfg.tg_api_id,
        cfg.tg_api_hash,
        proxy=proxy,
        auto_reconnect=True,
        connection_retries=5,
        request_retries=3,
        timeout=15,
        flood_sleep_threshold=60,
        sequential_updates=True,
    )


def convert(message, chat_id, entity=None, analyze=True):
    link = None
    username = getattr(entity, "username", None)
    if username:
        link = f"https://t.me/{username}/{message.id}"
    else:
        bare_id, peer_type = utils.resolve_id(chat_id)
        if peer_type is PeerChannel:
            link = f"https://t.me/c/{bare_id}/{message.id}"
    return Message(
        chat_id=chat_id,
        message_id=message.id,
        message_date=stamp(message.date),
        message_text=message.message or "",
        sender_id=message.sender_id,
        reply_to_id=message.reply_to_msg_id,
        has_media=bool(message.media),
        edited_at=stamp(message.edit_date) if message.edit_date else None,
        message_link=link,
        analyze=analyze,
    )


class Collector:
    def __init__(self, client, writer, db, cfg):
        self.client, self.writer, self.db, self.cfg = client, writer, db, cfg
        self.entities = {}
        self.ids = {g.id for g in cfg.active_groups}
        self.fatal = asyncio.get_running_loop().create_future()

    async def prepare(self):
        # Resolve joined dialogs locally, no joining/inviting or sending to monitored groups.
        async for dialog in self.client.iter_dialogs():
            cid = utils.get_peer_id(dialog.entity)
            if cid in self.ids:
                self.entities[cid] = dialog.entity
        for cid in self.ids - self.entities.keys():
            try:
                self.entities[cid] = await self.client.get_entity(cid)
            except Exception as exc:
                await self.db.execute(
                    "INSERT INTO sync_state(chat_id,last_error) VALUES(?,?) ON CONFLICT(chat_id) DO UPDATE SET last_error=excluded.last_error",
                    (cid, f"group_unavailable:{type(exc).__name__}"),
                )
                log.warning("群组不可访问 chat=%s error=%s", cid, type(exc).__name__)
        if not self.entities:
            raise RuntimeError("所有目标群组均不可访问，请检查用户账号与群组 ID")
        self.client.add_event_handler(self.on_message, events.NewMessage(chats=list(self.entities)))
        self.client.add_event_handler(
            self.on_message, events.MessageEdited(chats=list(self.entities))
        )
        self.client.add_event_handler(self.on_delete, events.MessageDeleted())

    async def on_message(self, event):
        try:
            if event.chat_id in self.entities:
                message = convert(event.message, event.chat_id, self.entities[event.chat_id])
                try:
                    # Telethon's internal update queue is not bounded. Disconnect rather than
                    # hold it indefinitely when our own bounded queue cannot drain.
                    await asyncio.wait_for(self.writer.submit(message), timeout=5)
                except TimeoutError:
                    raise RuntimeError("ingestion_overload_restart_and_backfill") from None
        except Exception as exc:
            if not self.fatal.done():
                self.fatal.set_result(exc)

    async def on_delete(self, event):
        # Basic-group deletion updates may omit chat_id: never guess across chats.
        if event.chat_id in self.entities:
            try:
                await self.db.mark_deleted(event.chat_id, event.deleted_ids)
            except Exception as exc:
                if not self.fatal.done():
                    self.fatal.set_result(exc)

    async def watch(self):
        disconnect = asyncio.create_task(self.client.run_until_disconnected())
        done, _ = await asyncio.wait((disconnect, self.fatal), return_when=asyncio.FIRST_COMPLETED)
        try:
            if self.fatal in done:
                raise self.fatal.result()
            await disconnect
            raise ConnectionError("Telegram disconnected; restart required")
        finally:
            disconnect.cancel()
            await asyncio.gather(disconnect, return_exceptions=True)
