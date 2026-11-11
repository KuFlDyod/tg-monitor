import hashlib
import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime


def utcnow() -> str:
    return datetime.now(UTC).isoformat(timespec="microseconds")


def stamp(value: datetime) -> str:
    return value.astimezone(UTC).isoformat(timespec="microseconds")


def digest(value) -> str:
    return hashlib.sha256(
        json.dumps(value, ensure_ascii=False, sort_keys=True).encode()
    ).hexdigest()


@dataclass
class Message:
    chat_id: int
    message_id: int
    message_date: str
    message_text: str = ""
    sender_id: int | None = None
    reply_to_id: int | None = None
    has_media: bool = False
    edited_at: str | None = None
    message_link: str | None = None
    deleted: bool = False
    analyze: bool = True

    def values(self):
        return asdict(self)

    @property
    def revision(self):
        return digest([self.message_text, self.edited_at, self.deleted])
