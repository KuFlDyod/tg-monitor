import asyncio
import json
import time
from datetime import UTC, datetime

import httpx

from .models import digest, stamp, utcnow


class AIError(Exception):
    def __init__(self, code, retry_after=60):
        super().__init__(code)
        self.retry_after = retry_after


class AIClient:
    def __init__(self, db, cfg, client=None):
        self.db, self.cfg = db, cfg
        self.client = client or httpx.AsyncClient(
            timeout=cfg.ai_timeout, proxy=cfg.http_proxy or None
        )
        self.semaphore = asyncio.Semaphore(cfg.ai_max_concurrency)

    async def close(self):
        await self.client.aclose()

    async def _reserve(self):
        now = datetime.now(UTC)
        start = stamp(now.replace(hour=0, minute=0, second=0, microsecond=0))
        async with self.db.transaction() as conn:
            cur = await conn.execute("SELECT COUNT(*) FROM ai_usage WHERE created_at>=?", (start,))
            count = (await cur.fetchone())[0]
            if count >= self.cfg.ai_daily_budget:
                tomorrow = (
                    now.replace(hour=0, minute=0, second=0, microsecond=0).timestamp() + 86400
                )
                raise AIError("daily_budget_exhausted", max(1, tomorrow - time.time()))
            cur = await conn.execute("INSERT INTO ai_usage(created_at) VALUES(?)", (utcnow(),))
            return cur.lastrowid

    async def complete(self, system, data, validator):
        if not self.cfg.ai_enabled:
            raise AIError("ai_disabled", 3600)
        # UTF-8 byte count is a deliberately conservative upper bound for common tokenizers.
        body = json.dumps(data, ensure_ascii=False, separators=(",", ":"))
        if len((system + body).encode()) + 256 > self.cfg.ai_input_tokens:
            raise AIError("input_budget_exceeded", 3600)
        key = digest(
            [self.cfg.ai_base_url, self.cfg.ai_model, system, body, self.cfg.ai_output_tokens]
        )
        async with self.semaphore:
            cached = await self.db.fetchone("SELECT content FROM ai_cache WHERE key=?", (key,))
            if cached:
                return validator(json.loads(cached["content"]))
            payload = {
                "model": self.cfg.ai_model,
                "messages": [
                    {"role": "system", "content": system},
                    {"role": "user", "content": body},
                ],
                self.cfg.ai_token_parameter: self.cfg.ai_output_tokens,
            }
            if self.cfg.ai_json_mode:
                payload["response_format"] = {"type": "json_object"}
            for attempt in range(3):
                usage_id = await self._reserve()
                delay = 2**attempt
                try:
                    response = await self.client.post(
                        self.cfg.ai_base_url.rstrip("/") + "/chat/completions",
                        headers={"Authorization": "Bearer " + self.cfg.ai_api_key},
                        json=payload,
                    )
                    if response.status_code == 429:
                        try:
                            delay = min(
                                86400, max(1, float(response.headers.get("retry-after", delay)))
                            )
                        except ValueError:
                            pass
                        raise AIError("http_429", delay)
                    if response.status_code >= 400:
                        raise AIError(
                            f"http_{response.status_code}",
                            3600 if response.status_code < 500 else delay,
                        )
                    result = response.json()
                    usage = result.get("usage", {})
                    await self.db.execute(
                        "UPDATE ai_usage SET prompt_tokens=?,completion_tokens=?,status='received' WHERE id=?",
                        (
                            int(usage.get("prompt_tokens") or 0),
                            int(usage.get("completion_tokens") or 0),
                            usage_id,
                        ),
                    )
                    content = result["choices"][0]["message"]["content"]
                    checked = validator(json.loads(content))
                    await self.db.execute(
                        "INSERT OR REPLACE INTO ai_cache VALUES(?,?,?)",
                        (key, json.dumps(checked, ensure_ascii=False), utcnow()),
                    )
                    await self.db.execute(
                        "UPDATE ai_usage SET status='success' WHERE id=?", (usage_id,)
                    )
                    return checked
                except asyncio.CancelledError:
                    raise
                except (
                    httpx.HTTPError,
                    ValueError,
                    KeyError,
                    IndexError,
                    TypeError,
                    AttributeError,
                    AIError,
                ) as exc:
                    await self.db.execute(
                        "UPDATE ai_usage SET status='failed' WHERE id=?", (usage_id,)
                    )
                    error = exc if isinstance(exc, AIError) else AIError(type(exc).__name__, delay)
                    if attempt == 2 or error.retry_after > 30:
                        raise error from None
                    await asyncio.sleep(error.retry_after)


def validate_alert(data):
    if not isinstance(data, dict):
        raise ValueError("AI alert must be an object")
    for field in ("relevant", "needs_alert"):
        if type(data.get(field)) is not bool:
            raise ValueError(field)
    if type(data.get("importance")) is not int or not 0 <= data["importance"] <= 100:
        raise ValueError("importance")
    if type(data.get("confidence")) not in (int, float) or not 0 <= data["confidence"] <= 1:
        raise ValueError("confidence")
    for field, limit in [("title", 200), ("summary", 2000), ("category", 100)]:
        if not isinstance(data.get(field), str) or not 1 <= len(data[field]) <= limit:
            raise ValueError(field)
    return {
        k: data[k]
        for k in (
            "relevant",
            "needs_alert",
            "importance",
            "confidence",
            "title",
            "summary",
            "category",
        )
    }
