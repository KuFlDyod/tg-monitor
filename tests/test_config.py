from pathlib import Path

import pytest

from app.config import Group, Rule, load_settings


@pytest.mark.parametrize("kwargs", [{"enabled": "false"}, {"id": True}, {"summary_enabled": 1}])
def test_group_strict_types(kwargs):
    with pytest.raises(ValueError):
        Group(**{"id": -1, "name": "test", **kwargs})


@pytest.mark.parametrize(
    "kwargs", [{"keywords": "优惠"}, {"ai_check": "false"}, {"chat_ids": [1]}, {"min_score": True}]
)
def test_rule_strict_types(kwargs):
    with pytest.raises(ValueError):
        Rule(**{"id": "r", "name": "test", "keywords": ["优惠"], **kwargs})


def test_example_loads_without_secrets():
    cfg = load_settings(
        "/nonexistent", str(Path(__file__).resolve().parents[1] / "config"), require_secrets=False
    )
    assert cfg.active_groups[0].id == -1001234567890


def test_remote_plain_http_ai_rejected(monkeypatch):
    monkeypatch.setenv("AI_BASE_URL", "http://remote.example/v1")
    with pytest.raises(ValueError, match="HTTPS"):
        load_settings(
            "/nonexistent",
            str(Path(__file__).resolve().parents[1] / "config"),
            require_secrets=False,
        )
