"""End-to-end offline demo. Transport is mocked: no credentials or network needed."""

import asyncio
import json
import sys
import tempfile
from datetime import UTC, datetime, timedelta
from pathlib import Path

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from app.aggregator import Aggregator
from app.ai_client import AIClient
from app.alerts import Alerts
from app.bot import Bot
from app.config import Group, Rule, Settings
from app.database import Database, Writer
from app.models import Message, stamp
from app.summarizer import Summarizer


async def main():
    with tempfile.TemporaryDirectory(prefix="tg-monitor-demo-") as folder:
        cfg = Settings(
            groups=[Group(-1000000000123, "离线演示群")],
            rules=[Rule("demo", "优惠提醒", keywords=["优惠"], aggregation_seconds=0)],
            bot_chat_id=42,
            allowed_user_ids=(42,),
            ai_api_key="fake",
            ai_base_url="https://mock.invalid/v1",
            ai_model="fake",
            bot_token="fake",
        )
        db = await Database(str(Path(folder) / "demo.db")).open()
        sent = []

        def fake_ai(request):
            payload = json.loads(request.content)
            data = json.loads(payload["messages"][1]["content"])
            if "items" in data:
                refs = []
                for item in data["items"]:
                    refs.extend(item.get("refs", [item.get("id")]))
                result = {
                    "text": "【模拟 AI】讨论包含低价 VPS 优惠及网络故障反馈，价格与故障原因均待核实。",
                    "refs": list(dict.fromkeys(refs))[:6],
                }
            else:
                result = {
                    "relevant": True,
                    "needs_alert": True,
                    "importance": 88,
                    "confidence": 0.85,
                    "category": "demo",
                    "title": "【模拟 AI】VPS 优惠",
                    "summary": "演示优惠：月付 3.5 美元；库存和线路质量待核实。",
                }
            return httpx.Response(
                200,
                json={
                    "choices": [{"message": {"content": json.dumps(result, ensure_ascii=False)}}],
                    "usage": {"prompt_tokens": 100, "completion_tokens": 60},
                },
            )

        def fake_bot(request):
            sent.append(json.loads(request.content)["text"])
            return httpx.Response(200, json={"ok": True, "result": {"message_id": len(sent)}})

        ai = AIClient(db, cfg, httpx.AsyncClient(transport=httpx.MockTransport(fake_ai)))
        bot = Bot(db, cfg, client=httpx.AsyncClient(transport=httpx.MockTransport(fake_bot)))
        writer = Writer(db, interval=0.01)
        worker = asyncio.create_task(writer.run())
        try:
            now = datetime.now(UTC)
            for mid, text in enumerate(
                [
                    "VPS 优惠，香港机器月付 $3.5，https://example.com/vps",
                    "有人反馈线路丢包，原因待核实",
                    "普通技术讨论",
                ],
                1,
            ):
                await writer.submit(
                    Message(
                        -1000000000123,
                        mid,
                        stamp(now),
                        text,
                        message_link=f"https://t.me/c/123/{mid}",
                    )
                )
            await writer.queue.join()
            await Aggregator(db, cfg).step()
            await Alerts(db, cfg, ai).step()
            await db.queue_report(
                "daily:demo",
                42,
                stamp(now - timedelta(hours=24)),
                stamp(now + timedelta(minutes=1)),
            )
            await Summarizer(db, cfg, ai).step()
            while await bot.send_step():
                pass
            for text in sent:
                print(text + "\n" + "─" * 40)
            print(
                f"离线演示通过：3 条消息入库，{len(sent)} 条模拟通知。未连接 Telegram 或真实 AI。"
            )
            assert len(sent) == 2
        finally:
            worker.cancel()
            await asyncio.gather(worker, return_exceptions=True)
            await bot.close()
            await ai.close()
            await db.close()


if __name__ == "__main__":
    asyncio.run(main())
