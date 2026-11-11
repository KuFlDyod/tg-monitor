from datetime import datetime

from .models import stamp


async def status(db, cfg, queue_size=0, connected=False):
    local = datetime.now(cfg.zone).replace(hour=0, minute=0, second=0, microsecond=0)
    since = stamp(local)
    messages = await db.fetchone(
        "SELECT COUNT(*) AS count,MAX(collected_at) AS last FROM messages WHERE collected_at>=?",
        (since,),
    )
    latest = await db.fetchone("SELECT MAX(collected_at) AS last FROM messages")
    ai = await db.fetchone(
        "SELECT COUNT(*) AS n,COALESCE(SUM(prompt_tokens+completion_tokens),0) AS tokens FROM ai_usage WHERE created_at>=?",
        (since,),
    )
    pending = await db.fetchone(
        "SELECT COUNT(*) AS n FROM notification_outbox WHERE status='pending'"
    )
    events = await db.fetchone("SELECT COUNT(*) AS n FROM alert_events WHERE status='pending'")
    reports = await db.fetchone("SELECT COUNT(*) AS n FROM summaries WHERE status='pending'")
    last = await db.fetchone("SELECT MAX(period_end) AS t FROM summaries WHERE status='done'")
    errors = await db.fetchall(
        "SELECT chat_id,last_error FROM sync_state WHERE last_error IS NOT NULL"
    )
    ai_error = await db.fetchone(
        "SELECT last_error FROM alert_events WHERE status='pending' AND last_error IS NOT NULL ORDER BY id DESC LIMIT 1"
    )
    report_error = await db.fetchone(
        "SELECT last_error FROM summaries WHERE status='pending' AND last_error IS NOT NULL ORDER BY id DESC LIMIT 1"
    )
    bot_error = await db.get_state("bot_last_error", "无")
    return (
        f"Telegram：{'已连接' if connected else '未连接'}\n监控群组：{len(cfg.active_groups)}\n"
        f"今日入库：{messages['count']}\n最近入库 UTC：{latest['last'] or '无'}\n"
        f"写入队列：{queue_size}/{cfg.message_queue_size}\n待分析事件：{events['n']}\n"
        f"待发送：{pending['n']}\n待汇总：{reports['n']}\n最近报告 UTC：{last['t'] or '无'}\n"
        f"今日 AI 尝试：{ai['n']}，Token：{ai['tokens']}\n"
        f"AI 每日预算：{cfg.ai_daily_budget}（UTC 日界）\n补采错误：{errors or '无'}\n"
        f"AI 错误：{(ai_error or report_error or {}).get('last_error', '无')}\nBot 错误：{bot_error}"
    )
