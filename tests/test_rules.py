import pytest

from app.config import Rule
from app.rules import event_key, matches


@pytest.mark.parametrize(
    "text,expected", [("优惠 VPS 月付", True), ("优惠 VPS 广告", False), ("VPS 月付", False)]
)
def test_keywords_blacklist(text, expected):
    rule = Rule("r", "r", keywords=["优惠"], all_keywords=["VPS"], blacklist=["广告"])
    assert matches(rule, -1, text) is expected


def test_regex_and_chat_scope():
    rule = Rule("r", "r", patterns=[r"\bVPS\b"], chat_ids=[-1])
    assert matches(rule, -1, "vps sale")
    assert not matches(rule, -2, "vps sale")


def test_domain_boundary():
    rule = Rule("r", "r", domains=["example.com"])
    assert matches(rule, -1, "https://shop.example.com/sale")
    assert not matches(rule, -1, "https://example.com.evil.net")


@pytest.mark.parametrize(
    "text,expected",
    [
        ("VPS 月付 $3.5", True),
        ("VPS 年付 $3.5", False),
        ("VPS 月付 3元", False),
        ("VPS 月付 $6", False),
        ("VPS 3.5", False),
        ("VPS 月付 $3 或年付 $20", False),
    ],
)
def test_price_currency_period(text, expected):
    rule = Rule(
        "r", "r", keywords=["VPS"], price_max=4, currency="USD", period="month", ai_check=False
    )
    assert matches(rule, -1, text) is expected


def test_ambiguous_price_goes_to_ai():
    rule = Rule("r", "r", keywords=["VPS"], price_max=4, currency="USD", period="month")
    assert matches(rule, -1, "VPS价格不明")


def test_event_url_fingerprint():
    assert event_key("优惠 https://example.com/a") == event_key("补货 https://example.com/a")


def test_regex_timeout():
    rule = Rule("r", "r", patterns=["(a+)+$"])
    assert not matches(rule, -1, "a" * 50000 + "!")
