import argparse
import asyncio
import fcntl
import logging
import os
import signal
import sys
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path

from .aggregator import Aggregator
from .ai_client import AIClient
from .alerts import Alerts
from .bot import Bot
from .collector import Collector, create_client
from .config import load_settings
from .database import Database, Writer
from .metrics import status
from .scheduler import Scheduler
from .summarizer import Summarizer
from .sync import Sync

log = logging.getLogger(__name__)


def configure_logs(cfg):
    Path(cfg.log_dir).mkdir(parents=True, exist_ok=True, mode=0o700)
    handlers = [
        logging.StreamHandler(),
        TimedRotatingFileHandler(
            Path(cfg.log_dir) / "monitor.log", when="midnight", backupCount=14, utc=True
        ),
    ]
    logging.basicConfig(
        level=cfg.log_level,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        handlers=handlers,
    )
    # Never log HTTP request URLs; Bot token is embedded in path.
    for name in ("httpx", "httpcore", "telethon"):
        logging.getLogger(name).setLevel(logging.CRITICAL)


async def run_service(cfg):
    db = await Database(cfg.database_path).open()
    client, ai, bot = create_client(cfg), AIClient(db, cfg), None
    tasks = []
    try:
        await client.connect()
        if not await client.is_user_authorized():
            raise RuntimeError("Telegram 会话未授权：先运行 python -m app.main login")
        writer = Writer(db, cfg.message_queue_size, cfg.db_batch_size, cfg.db_flush_interval)
        collector = Collector(client, writer, db, cfg)
        await collector.prepare()
        sync = Sync(client, db, writer, cfg, collector.entities)
        bot = Bot(db, cfg, writer, collector)
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, stop.set)
        services = {
            "writer": writer.run(),
            "collector": collector.watch(),
            "sync": sync.run(),
            "aggregation": Aggregator(db, cfg).run(),
            "alerts": Alerts(db, cfg, ai).run(),
            "summaries": Summarizer(db, cfg, ai).run(),
            "scheduler": Scheduler(db, cfg).run(),
            "sender": bot.sender(),
            "poller": bot.poll(),
            "commands": bot.commands(),
        }
        tasks = [asyncio.create_task(coro, name=name) for name, coro in services.items()]
        stopper = asyncio.create_task(stop.wait(), name="stop")
        tasks.append(stopper)
        log.info("监控已启动：groups=%d", len(cfg.active_groups))
        done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        failure = next((task for task in done if task is not stopper), None)
        if failure:
            error = failure.exception()
            log.error(
                "关键任务终止 task=%s error=%s",
                failure.get_name(),
                type(error).__name__ if error else "unexpected_exit",
            )
            raise RuntimeError(f"关键任务 {failure.get_name()} 停止") from None
        # Stop new input, then allow accepted messages to commit before cancelling workers.
        await client.disconnect()
        try:
            await asyncio.wait_for(writer.queue.join(), timeout=15)
        except TimeoutError:
            log.warning("写入队列未完全排空，下次按持久化历史游标补采")
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await client.disconnect()
        if bot:
            await bot.close()
        await ai.close()
        await db.close()


async def utility(args, cfg):
    if args.command in ("login", "groups"):
        if not cfg.tg_api_id or not cfg.tg_api_hash:
            raise ValueError("请先配置 TG_API_ID 和 TG_API_HASH")
        client = create_client(cfg)
        try:
            if args.command == "login":
                await client.start(
                    phone=cfg.tg_phone or (lambda: input("Telegram 手机号（含国家码）："))
                )
                print("登录完成；Session 等同账号凭据，请勿分享。")
            else:
                await client.connect()
                if not await client.is_user_authorized():
                    raise ValueError("请先执行 login")
                async for dialog in client.iter_dialogs():
                    if dialog.is_group:
                        print(f"{dialog.id}\t{dialog.name}")
        finally:
            await client.disconnect()
        return
    db = await Database(cfg.database_path).open()
    try:
        if args.command == "backup":
            await db.backup(args.output)
            print("备份完成：", args.output)
        elif args.command == "status":
            print(await status(db, cfg))
        elif args.command == "init-db":
            print("数据库已初始化：", cfg.database_path)
    finally:
        await db.close()


def main():
    os.umask(0o077)
    parser = argparse.ArgumentParser(description="Telegram 群组 AI 监控（单进程）")
    parser.add_argument(
        "command",
        nargs="?",
        choices=["run", "login", "groups", "init-db", "check", "status", "backup"],
        default="run",
    )
    parser.add_argument("--env", default=".env")
    parser.add_argument("--config-dir", default="config")
    parser.add_argument("--output", default="data/backups/manual.db")
    args = parser.parse_args()
    try:
        cfg = load_settings(
            args.env, args.config_dir, require_secrets=args.command in ("run", "check")
        )
        if args.command == "check":
            print("配置校验通过（不验证外部网络或凭据有效性）。")
            return
        Path(cfg.database_path).parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        # All CLI commands share this lock: no second process may reset an active event.
        with open(cfg.database_path + ".lock", "a") as lock:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise ValueError(
                    "服务正在运行；请通过 Bot 查询状态，维护命令前先停止服务"
                ) from None
            configure_logs(cfg)
            asyncio.run(run_service(cfg) if args.command == "run" else utility(args, cfg))
    except (ValueError, OSError, RuntimeError) as exc:
        # Own validation messages only; network exceptions may contain credentials.
        print(
            f"启动失败：{exc}"
            if isinstance(exc, (ValueError, RuntimeError))
            else f"启动失败：{type(exc).__name__}",
            file=sys.stderr,
        )
        raise SystemExit(1)
    except Exception as exc:
        print(f"启动失败：{type(exc).__name__}（检查凭据、代理和服务日志）", file=sys.stderr)
        raise SystemExit(1)


if __name__ == "__main__":
    main()
