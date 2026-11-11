# Telegram 群组 AI 监控系统

依据《Telegram 群组 AI 监控系统——轻量化技术方案 V1.0》实现的 Python 后台服务。

**定位：1–5 个群组、一个普通 Telegram 用户账号、一个私聊通知 Bot、一个 SQLite 数据库。** 不含 Web 后台，不使用 Redis、Celery、MySQL 或本地大模型。

> 交付状态：核心代码、离线演示、自动化测试与部署文件已完成。尚未使用真实账号登录，未发送真实通知，未调用真实 AI，未在 512MB VPS 做长期运行验收。请先完成下文的小范围实测，再长期运行。

## 1. 功能

- Telethon 用户账号登录、列出已加入群组；只监听配置中启用的群组，不自动加群或在群内发言。
- 新消息、编辑消息；删除事件尽力处理。保存媒体标记但不下载附件、不识别语音或图片。
- 有界队列、批量事务、SQLite WAL；消息唯一键去重。
- 首次同步最近 300 条；之后启动及每 120 秒进行增量补采。支持 FloodWait 和自动重连。
- 关键词、AND 关键词、黑名单、带超时的预编译正则、群组范围、URL/域名与价格条件。
- 同群同规则的短窗口聚合、文本/URL 去重、持久化冷却状态。
- OpenAI Chat Completions 兼容 HTTP 接口：结构化结果校验、缓存、并发上限、重试、每日预算与使用量。
- 持久化告警、报告任务、通知 outbox 和 Bot 命令 inbox。
- 默认 Asia/Shanghai 每日 21:00 汇总；支持分群发送或合并发送；全文分批与层级归并、来源 ID 校验、中间结果重用。
- 私聊 Bot：`/start`、`/help`、`/status`、`/groups`、`/search 关键词`、`/report`。
- UTC 存储、时区展示、滚动日志、定期 SQLite 备份和保留策略、systemd 单进程运行。

## 2. 先运行离线演示

要求 Python 3.11+，Linux。当前实际测试解释器为 Python 3.14.4；未声称已经在所有 Python/系统组合上测试。

```bash
cd telegram-monitor
python3 -m venv .venv
.venv/bin/pip install -r requirements-lock.txt
.venv/bin/python scripts/demo.py
```

该演示使用临时数据库和 HTTP MockTransport，不需要 `.env`、不发起外网请求；覆盖三条模拟消息入库、AI 告警、日报、Bot 模拟发送。模拟 AI 的内容与评分仅用于验证流程，不代表模型效果。参考 `DEMO_OUTPUT.txt`。

测试与压力测试：

```bash
.venv/bin/pip install -r requirements-test-lock.txt
.venv/bin/python -m pytest -q
.venv/bin/ruff check .
.venv/bin/ruff format --check .
.venv/bin/python scripts/benchmark.py --messages 100000 --output benchmark-local.json
```

`requirements.txt` 为依赖兼容范围，`requirements-lock.txt` 为本次测试的生产依赖版本，`requirements-test-lock.txt` 另含测试工具。锁文件不包含下载哈希；上线前仍需审查依赖并在目标主机验证安装。

## 3. 配置真实服务

### 3.1 准备凭据

1. 准备自己的 Telegram API ID / API Hash，以及有权访问目标群组的普通用户账号。
2. 使用 Telegram 官方 BotFather 创建独立通知 Bot。
3. **先从通知接收人的 Telegram 私聊向该 Bot 发送 `/start`**，否则 Bot 可能无法主动通知。
4. 获取接收人的 Telegram **用户 ID**，填入 `BOT_CHAT_ID`；不是手机号、用户名或群组 ID。
5. 配置选定供应商的 AI API Key、兼容地址和模型名。本项目不预设付费模型。

不要将手机号、验证码、二步验证密码、API Key 或 Session 发到聊天/仓库。只在本地受保护的配置文件和登录终端中填写。

```bash
cp .env.example .env
chmod 600 .env
mkdir -p data logs
chmod 700 data logs
# 编辑 .env，填写下列字段
```

最小必填项：

```ini
TG_API_ID=你的整数ID
TG_API_HASH=你的APIHash
TG_PHONE=含国家码的手机号
BOT_TOKEN=你的BotToken
BOT_CHAT_ID=接收人的正整数用户ID
AI_ENABLED=true
AI_API_KEY=你的APIKey
AI_BASE_URL=https://供应商域名/v1
AI_MODEL=供应商支持的模型名
```

`AI_BASE_URL` 是前缀，程序会追加 `/chat/completions`，不要重复写该路径。远程 AI 地址必须 HTTPS，本地回环地址可 HTTP。环境变量优先于 `.env`。

如果模型要求 `max_completion_tokens`，设置 `AI_TOKEN_PARAMETER=max_completion_tokens`；默认 `max_tokens` 用于兼容其他提供商。供应商不支持 JSON mode 时，可设置 `AI_JSON_MODE=false`，但模型仍必须返回符合约定的 JSON。

### 3.2 可选代理

```ini
HTTP_PROXY=http://127.0.0.1:7890
TG_PROXY=http://127.0.0.1:7890
```

- `HTTP_PROXY` 用于 Bot API 和 AI HTTP 请求。
- `TG_PROXY` 显式用于 Telethon MTProto，支持 `http`、`socks5`、`socks4`。
- 不需要代理时留空。示例中的回环地址只对运行服务的那台机器有效，部署 VPS 时不要盲目照搬本机配置。
- 如果终端安装依赖需要代理，按自身网络环境设置 `http_proxy`、`https_proxy`、`HTTP_PROXY`、`HTTPS_PROXY`、`all_proxy`、`ALL_PROXY`。

### 3.3 登录和选群

```bash
.venv/bin/python -m app.main login
.venv/bin/python -m app.main groups
```

登录交互会要求验证码，启用二步验证的账号还会要求密码。会话默认位于 `data/collector.session`。Session 等同登录凭据，必须保护并以运行服务的同一系统用户创建。

将列表输出的负整数群组 ID 填入 `config/groups.yaml`：

```yaml
groups:
  - id: -1001234567890  # 替换成真实 ID
    name: 我的监控群
    enabled: true
    summary_enabled: true
    alert_enabled: true
```

1–5 个启用群组；可保留更多停用配置。配置变更需要重启服务。

```bash
.venv/bin/python -m app.main check
.venv/bin/python -m app.main init-db
.venv/bin/python -m app.main run
```

`check` 仅校验本地配置，不证明凭据有效或网络可达。终端按 Ctrl+C 优雅停止。

兼容方案中的脚本入口：`scripts/login.py`、`scripts/list_groups.py`、`scripts/init_db.py`。

## 4. 规则语义与 AI 降级

示例见 `config/rules.yaml`。规则字段之间是 **AND** 关系：

| 字段 | 语义 |
|---|---|
| `keywords` | 列表内任意一个匹配，忽略大小写 |
| `all_keywords` | 所有关键词都必须匹配 |
| `blacklist` | 任意一个命中即排除 |
| `patterns` | 任一正则匹配；每次匹配最多 20ms |
| `chat_ids` | 空列表表示全部启用且开启告警的群组 |
| `domains` | 精确域名或它的子域名，不接受 `example.com.evil.net` |
| `require_url` | 必须包含 HTTP(S) URL |
| `price_max/currency/period` | 价格上限、USD/CNY/EUR、month/year |
| `ai_check` | 是否需要 AI 二次确认 |
| `min_score` | AI 重要性阈值 |
| `aggregation_seconds` | 默认 20 秒，可设 0 立即分析 |
| `cooldown_seconds` | 同群、同规则在此期间最多一次告警，可能抑制不同事件 |

价格识别仅对“单一价格 + 明确币种 + 单一付款周期”做确定性判断；多价格、多周期或无法解析的候选交给 AI。`ai_check=false` 时无法可靠解析不会触发。

事件以同群同规则聚合，最多 30 条，并受字节预算约束。URL 或规范化文本完全相同可去重；**不包含嵌入模型语义去重或商品实体解析**。同一 URL 的不同参数目前视为不同 URL；冷却仍可抑制重复告警。

- AI 失败：事件/日报保留并退避重试，采集继续。
- 每日预算以 **UTC 日界**计算，统计每次实际 HTTP 尝试，包括失败请求；超额任务等下一日重试。
- `AI_ENABLED=false`：保留采集、搜索和 `ai_check=false` 的确定性告警；语义规则不会被冒充成确定性结果。已经处理为 `ai_disabled` 的事件不自动重放。
- AI 不能执行命令或修改配置；群消息按不可信数据输入。只发送必要文本和消息引用 ID，不发送 sender_id。
- 告警超长候选会截断并加“截断”标记；**日报会切分长文本并覆盖全部文本，不只取前 N 条**。
- `AI_INPUT_TOKENS` 实际采用更保守的 UTF-8 字节预算估算，非供应商精确分词。真实 Token 统计来自 API `usage`，供应商不返回用量时记录为 0。

## 5. 日报和搜索

### 日报

- 默认每日 21:00 Asia/Shanghai；首次在当天定时点之前启动会等待该时点，不立即发送昨天的报告。
- 自动日报按 **入库时间窗口**统计，以免网络中断后补采的旧消息永久漏报。报告会明确标注口径；首次历史同步的旧消息可能出现在首次日报。
- `/report` 按 **消息发送时间**汇总命令触发前 24 小时，与自动日报口径不同。
- 日报创建处理快照，后续编辑/新增不会改变正在生成的日报。已完成的中间摘要持久化，失败后复用。
- 所有消息分批后采用磁盘存储的二叉归并摘要；归并是有损压缩，但不会故意只处理前 N 条。
- AI 引用必须属于输入 ID 集合，最终链接由数据库生成；每个群组最终保留最多 6 个引用。
- `REPORT_COMBINED=false` 分群发送，`true` 按群组分节合并发送。
- 任务状态 `done` 表示已生成并进入 outbox，不等于 Telegram 已送达；送达状态看 `notification_outbox`。
- 停机多日后每 30 秒补建一个遗漏的日报窗口，失败窗口独立重试。更换时区或定时点前，先检查已有窗口，必要时维护 `report_schedule_end` 状态。

### 搜索与权限

`/search` 默认搜索启用群组近 30 天最新 10000 条未删除消息，最多返回 10 条；SQL 参数化并转义 `%`、`_`。未启用 FTS5，不承诺无限历史全文搜索。

Bot 必须同时满足：
- `from.id` 在 `ALLOWED_USER_IDS` 中（默认只有 `BOT_CHAT_ID`）；
- `chat.type` 为 `private`；
- 私聊 ID 与调用用户 ID 一致。

未授权消息不执行命令，不保存其内容。新增授权用户即允许其读取所有启用群组的搜索结果，请谨慎配置。自动告警/日报只发送到 `BOT_CHAT_ID`；手动报告返回给调用者。

## 6. 可靠性边界

### 游标与重启

- 消息唯一键为 `(chat_id, message_id)`。
- **实时入库绝不推进历史同步游标**，防止高 ID 新消息掩盖断线期间的旧消息。
- 增量补采严格升序分页，批次成功提交后才推进游标；写入失败不推进。
- 初始最近 N 条采用分页倒序读取，但整个初始扫描完成后才设置游标，半途退出可安全重复。
- Telegram 的数字 ID 缺口可能来自删除、权限或不可见内容，不代表程序一定丢了消息。本服务只恢复账号/API 当前可访问的历史，不承诺完整恢复。
- 离线期间的旧消息编辑/删除不保证被增量 ID 扫描补齐。删除事件缺少群组 ID 时不会猜测，以免误删其他群消息。
- 若群组启动时不可访问，会记录错误；恢复权限后需重启以重新解析该群组。
- 应用队列满后背压；超过 5 秒无法入队会触发进程退出，由 systemd 重启并补采，而非无限累积 Telethon 内部更新队列。
- SQLite WAL 使用 `synchronous=NORMAL`，极端断电仍可能损失最近事务。

### 任务与通知

- 聚合状态、告警、日报、Bot inbox/outbox 均持久化。
- 告警生成和入 outbox 在一个 SQLite 事务内，去重键阻止正常重试重复创建通知。
- Bot 更新持久化后才推进 offset，执行命令与响应入队原子提交。
- **Bot API 与本地数据库无法跨系统原子提交**：如果 Telegram 已接受消息、进程却在本地标记完成前崩溃，可能重复发送；消息中带事件/报告编号辅助识别。不声称 exactly-once。
- 普通失败指数退避，429 尊重 `retry_after`。发送 400/403 放入 `dead` 状态并在 `/status` 提示，防止阻塞其余通知；修复原因后可人工重放。
- 单进程文件锁防止重复启动。CLI 维护命令与服务互斥；运行中查看状态请用 Bot `/status`。
- 单个关键后台任务异常退出时主进程失败退出，避免“采集任务已经死掉但进程仍健康”。预期网络/AI 错误则留在各自重试流程。

## 7. Debian 12 / Ubuntu 24.04 部署

以下命令是部署说明，交付时没有在你的机器执行安装系统服务或创建用户。

```bash
sudo apt update
sudo apt install -y python3 python3-venv python3-pip
sudo useradd --system --create-home --shell /bin/bash telegrammon
sudo mkdir -p /opt/telegram-monitor
# 将本项目所有代码（包括 .env.example）复制到 /opt/telegram-monitor
sudo chown -R telegrammon:telegrammon /opt/telegram-monitor
cd /opt/telegram-monitor
sudo -u telegrammon python3 -m venv .venv
sudo -u telegrammon .venv/bin/pip install -r requirements-lock.txt
sudo -u telegrammon cp .env.example .env
sudo chmod 600 .env
sudo -u telegrammon mkdir -p data logs
sudo chmod 700 data logs
sudo -u telegrammon nano .env
sudo -u telegrammon .venv/bin/python -m app.main login
sudo -u telegrammon .venv/bin/python -m app.main groups
sudo -u telegrammon nano config/groups.yaml
sudo -u telegrammon nano config/rules.yaml
sudo -u telegrammon .venv/bin/python -m app.main check
sudo -u telegrammon .venv/bin/python -m app.main init-db
sudo cp deploy/telegram-monitor.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now telegram-monitor
```

注意：`ProtectSystem=strict` 仅允许写 `/opt/telegram-monitor/data` 和 `logs`。自定义数据库/Session/日志目录时必须同步修改 unit 的 `ReadWritePaths`。`ProtectHome=true` 意味着会话不能放在 home 目录下。仅支持单实例，勿同时运行终端进程和 systemd 实例。

```bash
sudo systemctl status telegram-monitor
sudo journalctl -u telegram-monitor -f
sudo systemctl stop telegram-monitor
sudo systemctl start telegram-monitor
sudo systemctl restart telegram-monitor
sudo systemctl disable --now telegram-monitor
```

日志同时写入 journald 与 `logs/monitor.log`，应用日志按 UTC 日切分保留 14 份。HTTP 客户端请求日志被禁用，避免 URL 中 Bot Token 泄漏。

## 8. 备份、恢复、升级

每天首次维护执行 SQLite backup API，备份写临时文件后替换，保留 7 天。默认原始消息、已完成告警和已发送通知保留 90 天，AI 缓存/中间摘要/报告快照保留 30 天，最终日报长期保存。待报告窗口会推迟相关消息清理；AI 长期故障或通知长期积压会增加磁盘占用，应监控磁盘而非假定固定上限。

不会自动执行高负载 `VACUUM`。每轮原始消息清理最多 20000 条，超大量过期数据可能跨多个维护日清理。

手动备份（维护命令要求先停止服务）：

```bash
sudo systemctl stop telegram-monitor
cd /opt/telegram-monitor
sudo -u telegrammon .venv/bin/python -m app.main backup --output data/backups/manual.db
sudo systemctl start telegram-monitor
```

恢复：停止服务，保留当前数据库以便回退，将备份复制到 `DATABASE_PATH`；旧的同名 `-wal`/`-shm` 只能在服务确认停止后移走，然后检查权限再启动。**不要只复制正在运行的主 `.db` 文件**作为备份。恢复旧备份可能重复补采/发送，请检查 outbox 后再上线。

升级：
1. 停止服务并备份数据库。
2. 单独备份 `.env`、`config/`、Session；不要放入公开压缩包。
3. 替换代码，不覆盖 `data/`、`logs/`、`.env` 和个人配置。
4. 更新依赖、运行测试和 `check`，再启动服务。
5. 检查 `/status`、补采进度及一条真实测试通知；失败则回退代码及匹配的备份。

当前 schema version 为 1，不支持从任意第三方数据库迁移。程序拒绝打开标记为更高版本的数据库。不要假定未来数据库变更可以无损直接降级。

## 9. 故障排查

| 现象 | 检查 |
|---|---|
| 无法登录/会话失效 | 使用同一系统用户运行 `login`；检查手机号、验证码、二步验证、Session 权限及 TG_PROXY |
| 群消息没有入库 | 群 ID 是否来自 `groups`，用户是否仍在群中，enabled 是否开启；查看补采错误 |
| 已入库但不告警 | 首次历史默认不告警；核对规则、黑名单、冷却、AI 分数/预算；检查事件状态 |
| AI `http_401`/`http_403` | 检查密钥和模型权限；错误按小时退避，不会打印密钥 |
| AI `http_400` | 核对模型参数、JSON mode、token 参数名及 base URL；兼容 API 不代表所有参数都相同 |
| AI `input_budget_exceeded` | 检查超长规则名、特殊字符；调大输入预算或缩小事件内容 |
| Bot `bot_403`/dead letter | 接收人先私聊 `/start`，检查是否屏蔽 Bot、ID 是否正确；修复后人工重放 dead 通知 |
| Bot `bot_409` | 检查是否另有程序轮询同一 Bot 或设置了 webhook；本服务只支持 getUpdates 轮询 |
| Bot `bot_429` | 等待 retry_after，不要重启刷请求 |
| 日报不出现 | 核对时区、定时点、summary_enabled、AI 错误/预算和 outbox 状态 |
| 数据库锁/无法写入 | 检查只运行一个进程、磁盘容量、权限、systemd 写入路径；不删除正在使用的 WAL |
| 内存/磁盘上涨 | 检查队列、待处理事件、快照/缓存、日志和积压窗口；对比真实群消息量 |

`/status` 的今日消息和 AI 请求数按展示时区统计，AI 预算重置按 UTC；临界时刻两者可能不同。`AI usage` 未返回时不能用 0 Token 推断免费。

需要人工重放 dead 通知时，停服务、备份后，用 SQLite 只将已核实需要重试的行改为 `status='pending', next_retry_at=0`；不要批量重置所有已发送通知。

## 10. 目录与验收

```text
app/                 采集、补采、数据库、规则、AI、告警、摘要、Bot、定时维护
config/              群组与规则示例
scripts/             登录、列群、初始化、离线演示、压力测试
tests/              自动化测试（不需要真实 API）
deploy/              systemd unit
.env.example         配置模板，不含真实凭据
TEST_REPORT.md       本次测试结果与未验证项
BENCHMARK.json       本机离线 SQLite 压力测试数据
DEMO_OUTPUT.txt      离线端到端演示输出
```

正式上线前至少验证：
- [ ] 仅加入/监控自己有访问权限且适合处理的群组；确认向第三方 AI 发送群聊文本符合自身使用要求。
- [ ] 新消息、编辑、可观察的删除，以及 1–5 个群组的路由。
- [ ] 首次同步、断网 10 分钟后补采、进程异常退出后的去重。
- [ ] `/start` 后收到真实通知，未授权账号无法搜索或触发报告。
- [ ] AI 真实 JSON 输出、用量、错误重试、日报来源可靠性。
- [ ] 真实 21:00 定时任务和跨日重启。
- [ ] 在目标 VPS 上做 24–72 小时内存/磁盘监控，再评估 512MB 部署。

开发依据：本地需求方案；Telegram Bot API 官方规范、HTTPX 代理接口文档、OpenAI Chat Completions 官方接口规范，以及实际安装的 Telethon 1.45.0 API 文档/源码。未复制方案列出的第三方监控项目代码。
