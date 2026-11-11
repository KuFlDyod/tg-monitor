import re
from urllib.parse import urlparse

from .models import digest

URL_RE = re.compile(r'https?://[^\s<>"\u3000]+', re.IGNORECASE)
PRICE_RE = re.compile(
    r"(?:(USD|CNY|RMB|EUR|[$¥￥€])\s*(\d+(?:\.\d+)?)|(\d+(?:\.\d+)?)\s*(美元|美金|刀|元|欧元))",
    re.IGNORECASE,
)
CURRENCIES = {
    "$": "USD",
    "USD": "USD",
    "美元": "USD",
    "美金": "USD",
    "刀": "USD",
    "¥": "CNY",
    "￥": "CNY",
    "CNY": "CNY",
    "RMB": "CNY",
    "元": "CNY",
    "€": "EUR",
    "EUR": "EUR",
    "欧元": "EUR",
}


def urls(text):
    return [url.rstrip(".,，。;；!！)）]") for url in URL_RE.findall(text)]


def event_key(text):
    links = urls(text)
    if links:
        normalized = sorted({url.lower().split("#")[0] for url in links})
    else:
        normalized = re.sub(r"\s+", "", text.casefold())
    return digest(normalized)


def matches(rule, chat_id, text):
    if not rule.enabled or (rule.chat_ids and chat_id not in rule.chat_ids) or not text.strip():
        return False
    lower = text.casefold()
    if any(k.casefold() in lower for k in rule.blacklist):
        return False
    if rule.keywords and not any(k.casefold() in lower for k in rule.keywords):
        return False
    if rule.all_keywords and not all(k.casefold() in lower for k in rule.all_keywords):
        return False
    try:
        if rule.compiled and not any(p.search(text, timeout=0.02) for p in rule.compiled):
            return False
    except TimeoutError:
        return False  # defend against catastrophic backtracking, even with local config
    links = urls(text)
    if rule.require_url and not links:
        return False
    if rule.domains:
        hosts = [(urlparse(url).hostname or "").lower() for url in links]
        if not any(
            h == d.lower() or h.endswith("." + d.lower()) for h in hosts for d in rule.domains
        ):
            return False
    if rule.price_max is not None:
        prices = list(PRICE_RE.finditer(text))
        periods = set()
        if re.search(r"月付|每月|/\s*(?:mo(?:nth)?|月)\b|per month", lower):
            periods.add("month")
        if re.search(r"年付|每年|/\s*(?:yr|year|年)\b|per year", lower):
            periods.add("year")
        # Multiple prices/cycles are ambiguous; defer to AI, never claim a local match.
        if len(prices) != 1 or len(periods) != 1:
            return rule.ai_check
        p = prices[0]
        currency = CURRENCIES[(p[1] or p[4]).upper()]
        amount = float(p[2] or p[3])
        return currency == rule.currency and rule.period in periods and amount <= rule.price_max
    return True
