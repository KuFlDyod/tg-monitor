from datetime import UTC, datetime, timedelta

from .models import stamp


async def search(db, cfg, term):
    term = term.strip()[:100]
    if not term:
        return "用法：/search 关键词"
    escaped = term.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
    since = stamp(datetime.now(UTC) - timedelta(days=cfg.search_days))
    groups = {g.id: g.name for g in cfg.active_groups}
    if not groups:
        return "没有启用的群组。"
    marks = ",".join("?" for _ in groups)
    # Bounded indexed time scan; result and scanned candidate count are both bounded.
    rows = await db.fetchall(
        f"""SELECT * FROM (
        SELECT * FROM messages WHERE chat_id IN ({marks}) AND message_date>=? AND deleted=0
        ORDER BY message_date DESC LIMIT 10000
        ) WHERE message_text LIKE ? ESCAPE '\\' LIMIT 10""",
        (*groups, since, "%" + escaped + "%"),
    )
    header = f"搜索：{term}\n范围：近 {cfg.search_days} 天最近 10000 条，最多 10 条结果。\n"
    if not rows:
        return header + "未找到匹配消息。"
    return header + "\n\n".join(
        f"【{groups[r['chat_id']]}】{datetime.fromisoformat(r['message_date']).astimezone(cfg.zone):%m-%d %H:%M}\n{r['message_text'][:250]}\n{r['message_link'] or str(r['chat_id']) + ':' + str(r['message_id'])}"
        for r in rows
    )
