"""Offline bounded-queue SQLite benchmark; no Telegram/AI network traffic."""

import argparse
import asyncio
import json
import resource
import sys
import tempfile
import time
from datetime import UTC, datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from app.database import Database, Writer
from app.models import Message, stamp


async def main(count, output):
    with tempfile.TemporaryDirectory(prefix="tg-monitor-benchmark-") as folder:
        db = await Database(str(Path(folder) / "load.db")).open()
        writer = Writer(db, 2000, 100, 0.01)
        task = asyncio.create_task(writer.run())
        start = time.perf_counter()
        cpu_start = time.process_time()
        max_queue = 0
        try:
            for i in range(1, count + 1):
                await writer.submit(
                    Message(
                        -1000000000123,
                        i,
                        stamp(datetime.now(UTC)),
                        f"模拟消息 {i}，VPS 技术讨论，无外部 API。",
                    )
                )
                max_queue = max(max_queue, writer.queue.qsize())
            await writer.queue.join()
            elapsed = time.perf_counter() - start
            actual = (await db.fetchone("SELECT COUNT(*) AS n FROM messages"))["n"]
            integrity = await db.fetchone("PRAGMA integrity_check")
            result = {
                "kind": "OFFLINE_SIMULATED_SQLITE_QUEUE_ONLY",
                "messages": actual,
                "elapsed_seconds": round(elapsed, 3),
                "messages_per_second": round(actual / elapsed, 1),
                "cpu_seconds": round(time.process_time() - cpu_start, 3),
                "peak_queue_size": max_queue,
                "queue_limit": 2000,
                "peak_rss_mib_linux": round(
                    resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024, 2
                ),
                "integrity_check": list(integrity.values())[0],
                "python": sys.version.split()[0],
                "limitations": "Not a 512MB VPS or real Telegram benchmark. No AI/network latency included.",
            }
            if actual != count:
                raise AssertionError("Message loss")
            text = json.dumps(result, ensure_ascii=False, indent=2)
            print(text)
            if output:
                Path(output).write_text(text + "\n")
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            await db.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--messages", type=int, default=100000)
    parser.add_argument("--output")
    args = parser.parse_args()
    asyncio.run(main(args.messages, args.output))
