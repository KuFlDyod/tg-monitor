from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from app.config import Group, Rule, Settings
from app.database import Database
from app.models import Message, stamp


@pytest.fixture
async def db(tmp_path):
    database = await Database(str(tmp_path / "test.db")).open()
    yield database
    await database.close()


@pytest.fixture
def cfg(tmp_path):
    return Settings(
        groups=[Group(-1000000000123, "测试群")],
        rules=[Rule("sale", "优惠", keywords=["优惠"], ai_check=False, aggregation_seconds=0)],
        bot_chat_id=42,
        allowed_user_ids=(42,),
        bot_token="secret-do-not-log",
        database_path=str(tmp_path / "test.db"),
        ai_api_key="test",
        ai_base_url="https://ai.example/v1",
        ai_model="mock",
    )


def message(mid=1, text="优惠 月付 $3 VPS", chat=-1000000000123, **kwargs):
    return Message(chat, mid, stamp(datetime.now(UTC)), text, **kwargs)


def telegram_message(mid=1, text="hello"):
    return SimpleNamespace(
        id=mid,
        message=text,
        date=datetime.now(UTC),
        sender_id=8,
        reply_to_msg_id=None,
        media=None,
        edit_date=None,
    )
