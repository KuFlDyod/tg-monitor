import os
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import urlparse
from zoneinfo import ZoneInfo

import regex
import yaml
from dotenv import load_dotenv


@dataclass
class Group:
    id: int
    name: str
    enabled: bool = True
    summary_enabled: bool = True
    alert_enabled: bool = True

    def __post_init__(self):
        if (
            type(self.id) is not int
            or self.id >= 0
            or not isinstance(self.name, str)
            or not self.name.strip()
        ):
            raise ValueError("群组需要负整数 ID 和非空名称")
        for name in ("enabled", "summary_enabled", "alert_enabled"):
            if type(getattr(self, name)) is not bool:
                raise ValueError(f"群组 {name} 必须是 YAML 布尔值，不要加引号")


@dataclass
class Rule:
    id: str
    name: str
    enabled: bool = True
    chat_ids: list[int] = field(default_factory=list)
    keywords: list[str] = field(default_factory=list)
    all_keywords: list[str] = field(default_factory=list)
    blacklist: list[str] = field(default_factory=list)
    patterns: list[str] = field(default_factory=list)
    domains: list[str] = field(default_factory=list)
    require_url: bool = False
    price_max: float | None = None
    currency: str | None = None
    period: str | None = None
    ai_check: bool = True
    min_score: int = 75
    cooldown_seconds: int = 300
    aggregation_seconds: int = 20
    compiled: list = field(default_factory=list, init=False, repr=False)

    def __post_init__(self):
        for name in ("enabled", "require_url", "ai_check"):
            if type(getattr(self, name)) is not bool:
                raise ValueError(f"规则 {name} 必须是 YAML 布尔值")
        for name in ("keywords", "all_keywords", "blacklist", "patterns", "domains"):
            value = getattr(self, name)
            if not isinstance(value, list) or not all(
                isinstance(x, str) and x.strip() for x in value
            ):
                raise ValueError(f"规则 {name} 必须为非空字符串列表")
        if not isinstance(self.chat_ids, list) or not all(
            type(x) is int and x < 0 for x in self.chat_ids
        ):
            raise ValueError("规则 chat_ids 必须为负整数列表")
        if not isinstance(self.name, str) or not self.name.strip() or not isinstance(self.id, str):
            raise ValueError("规则 ID 和名称必须为非空字符串")
        for name in ("min_score", "cooldown_seconds", "aggregation_seconds"):
            if type(getattr(self, name)) is not int:
                raise ValueError(f"规则 {name} 必须为整数")
        if not self.id or not 0 <= self.min_score <= 100:
            raise ValueError("规则 ID 不能为空，评分必须为 0–100")
        if self.cooldown_seconds < 0 or not 0 <= self.aggregation_seconds <= 300:
            raise ValueError("规则冷却/聚合时间非法")
        self.compiled = [regex.compile(p, regex.I) for p in self.patterns]
        if not any(
            [
                self.keywords,
                self.all_keywords,
                self.patterns,
                self.domains,
                self.require_url,
                self.price_max is not None,
            ]
        ):
            raise ValueError(f"规则 {self.id} 没有筛选条件")
        if self.price_max is not None:
            if (
                self.price_max <= 0
                or self.currency not in ("USD", "CNY", "EUR")
                or self.period not in ("month", "year")
            ):
                raise ValueError("价格规则需要正数 price_max、USD/CNY/EUR 币种及 month/year 周期")


@dataclass
class Settings:
    groups: list[Group] = field(default_factory=list)
    rules: list[Rule] = field(default_factory=list)
    tg_api_id: int = 0
    tg_api_hash: str = ""
    tg_phone: str = ""
    bot_token: str = ""
    bot_chat_id: int = 0
    allowed_user_ids: tuple[int, ...] = ()
    ai_api_key: str = ""
    ai_base_url: str = ""
    ai_model: str = ""
    ai_enabled: bool = True
    ai_timeout: int = 60
    ai_max_concurrency: int = 2
    ai_daily_budget: int = 200
    ai_input_tokens: int = 6000
    ai_output_tokens: int = 1200
    ai_json_mode: bool = True
    ai_token_parameter: str = "max_tokens"
    database_path: str = "data/telegram.db"
    session_path: str = "data/collector"
    http_proxy: str = ""
    tg_proxy: str = ""
    history_limit: int = 300
    message_queue_size: int = 2000
    db_batch_size: int = 100
    db_flush_interval: float = 1.0
    sync_interval: int = 120
    timezone: str = "Asia/Shanghai"
    report_hour: int = 21
    report_minute: int = 0
    report_combined: bool = True
    retention_days: int = 90
    cache_days: int = 30
    backup_days: int = 7
    search_days: int = 30
    log_level: str = "INFO"
    log_dir: str = "logs"

    @property
    def active_groups(self):
        return [g for g in self.groups if g.enabled]

    @property
    def zone(self):
        return ZoneInfo(self.timezone)


def boolean(value):
    if str(value).lower() not in ("true", "false", "1", "0"):
        raise ValueError(f"非法布尔值: {value}")
    return str(value).lower() in ("true", "1")


def load_settings(env_file=".env", config_dir="config", require_secrets=True) -> Settings:
    load_dotenv(env_file, override=False)
    defaults = Settings()
    values = {}
    for name in defaults.__dataclass_fields__:
        if name in ("groups", "rules", "allowed_user_ids"):
            continue
        raw = os.getenv(name.upper())
        if raw is not None:
            base = getattr(defaults, name)
            values[name] = boolean(raw) if isinstance(base, bool) else type(base)(raw)
    if os.getenv("ALLOWED_USER_IDS"):
        values["allowed_user_ids"] = tuple(
            int(x.strip()) for x in os.environ["ALLOWED_USER_IDS"].split(",")
        )
    for filename, cls, key in [("groups.yaml", Group, "groups"), ("rules.yaml", Rule, "rules")]:
        data = yaml.safe_load((Path(config_dir) / filename).read_text())
        if not isinstance(data, dict) or not isinstance(data.get(key), list):
            raise ValueError(f"{filename}: 必须包含 {key} 列表")
        try:
            values[key] = [cls(**row) for row in data[key]]
        except (TypeError, regex.error) as exc:
            raise ValueError(f"{filename}: 字段或正则格式错误 ({type(exc).__name__})") from None
    cfg = Settings(**values)
    if not 1 <= len(cfg.active_groups) <= 5:
        raise ValueError("必须启用 1–5 个群组")
    if any(type(g.id) is not int or g.id >= 0 for g in cfg.groups):
        raise ValueError("群组 ID 必须为负整数，请用 groups 命令获取")
    if len({g.id for g in cfg.groups}) != len(cfg.groups) or len({r.id for r in cfg.rules}) != len(
        cfg.rules
    ):
        raise ValueError("群组或规则 ID 重复")
    if not 0 <= cfg.report_hour <= 23 or not 0 <= cfg.report_minute <= 59:
        raise ValueError("日报时间非法")
    _ = cfg.zone
    if cfg.log_level not in ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"):
        raise ValueError("LOG_LEVEL 非法")
    if cfg.ai_token_parameter not in ("max_tokens", "max_completion_tokens"):
        raise ValueError("AI_TOKEN_PARAMETER 只能为 max_tokens 或 max_completion_tokens")
    for name in (
        "ai_timeout",
        "ai_max_concurrency",
        "ai_daily_budget",
        "ai_input_tokens",
        "ai_output_tokens",
        "message_queue_size",
        "db_batch_size",
        "sync_interval",
        "retention_days",
        "cache_days",
        "backup_days",
        "search_days",
    ):
        if getattr(cfg, name) <= 0:
            raise ValueError(f"{name} 必须大于 0")
    if not 1 <= cfg.history_limit <= 10000 or not 0.05 <= cfg.db_flush_interval <= 60:
        raise ValueError("HISTORY_LIMIT 应为 1–10000，DB_FLUSH_INTERVAL 应为 0.05–60")
    if cfg.ai_input_tokens < 2048 or cfg.ai_output_tokens < 256:
        raise ValueError("AI 输入预算至少 2048，输出预算至少 256")
    if cfg.ai_output_tokens * 2 > cfg.ai_input_tokens:
        raise ValueError("AI_OUTPUT_TOKENS 不得超过 AI_INPUT_TOKENS 的一半")
    if not cfg.allowed_user_ids and cfg.bot_chat_id > 0:
        cfg.allowed_user_ids = (cfg.bot_chat_id,)
    if require_secrets:
        if cfg.tg_api_id <= 0 or not cfg.tg_api_hash or not cfg.bot_token or cfg.bot_chat_id <= 0:
            raise ValueError("请填写 TG_API_ID、TG_API_HASH、BOT_TOKEN、正整数 BOT_CHAT_ID（私聊）")
        if any(uid <= 0 for uid in cfg.allowed_user_ids):
            raise ValueError("ALLOWED_USER_IDS 必须为正整数")
        if cfg.ai_enabled and not all((cfg.ai_api_key, cfg.ai_base_url, cfg.ai_model)):
            raise ValueError("AI_ENABLED=true 时必须填写 AI_API_KEY、AI_BASE_URL、AI_MODEL")
    if cfg.ai_base_url:
        parsed = urlparse(cfg.ai_base_url)
        if parsed.scheme != "https" and parsed.hostname not in ("localhost", "127.0.0.1", "::1"):
            raise ValueError("远程 AI_BASE_URL 必须使用 HTTPS")
    return cfg
