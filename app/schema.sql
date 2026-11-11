CREATE TABLE IF NOT EXISTS messages (
 chat_id INTEGER NOT NULL, message_id INTEGER NOT NULL, sender_id INTEGER,
 message_text TEXT NOT NULL, message_date TEXT NOT NULL, collected_at TEXT NOT NULL,
 edited_at TEXT, reply_to_id INTEGER, has_media INTEGER NOT NULL DEFAULT 0,
 message_link TEXT, deleted INTEGER NOT NULL DEFAULT 0, revision TEXT NOT NULL,
 analysis_pending INTEGER NOT NULL DEFAULT 1,
 PRIMARY KEY(chat_id,message_id)
);
CREATE INDEX IF NOT EXISTS idx_messages_date ON messages(chat_id,message_date);
CREATE INDEX IF NOT EXISTS idx_messages_collected ON messages(chat_id,collected_at,message_id);
CREATE INDEX IF NOT EXISTS idx_analysis ON messages(analysis_pending,collected_at);
CREATE TABLE IF NOT EXISTS sync_state (
 chat_id INTEGER PRIMARY KEY, last_message_id INTEGER NOT NULL DEFAULT 0,
 last_sync_at TEXT, last_error TEXT
);
CREATE TABLE IF NOT EXISTS alert_events (
 id INTEGER PRIMARY KEY AUTOINCREMENT, chat_id INTEGER NOT NULL, rule_id TEXT NOT NULL,
 event_key TEXT, payload TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'pending',
 created_at REAL NOT NULL, ready_at REAL NOT NULL, updated_at REAL NOT NULL,
 retry_count INTEGER NOT NULL DEFAULT 0, next_retry_at REAL NOT NULL DEFAULT 0,
 last_error TEXT, title TEXT, summary TEXT, importance INTEGER
);
CREATE INDEX IF NOT EXISTS idx_alert_pending ON alert_events(status,ready_at,next_retry_at);
CREATE TABLE IF NOT EXISTS alert_dedupe (
 key TEXT PRIMARY KEY, expires_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS notification_outbox (
 id INTEGER PRIMARY KEY AUTOINCREMENT, dedupe_key TEXT NOT NULL UNIQUE,
 chat_id INTEGER NOT NULL, message_text TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'pending',
 retry_count INTEGER NOT NULL DEFAULT 0, next_retry_at REAL NOT NULL DEFAULT 0,
 created_at TEXT NOT NULL, sent_at TEXT, last_error TEXT
);
CREATE INDEX IF NOT EXISTS idx_outbox_due ON notification_outbox(status,next_retry_at,id);
CREATE TABLE IF NOT EXISTS summaries (
 id INTEGER PRIMARY KEY AUTOINCREMENT, task_key TEXT NOT NULL UNIQUE,
 destination INTEGER NOT NULL, period_start TEXT NOT NULL, period_end TEXT NOT NULL,
 status TEXT NOT NULL DEFAULT 'pending', content TEXT, created_at TEXT NOT NULL,
 retry_count INTEGER NOT NULL DEFAULT 0, next_retry_at REAL NOT NULL DEFAULT 0,
 last_error TEXT
);
CREATE TABLE IF NOT EXISTS ai_cache (
 key TEXT PRIMARY KEY, content TEXT NOT NULL, created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS ai_usage (
 id INTEGER PRIMARY KEY AUTOINCREMENT, created_at TEXT NOT NULL,
 prompt_tokens INTEGER NOT NULL DEFAULT 0, completion_tokens INTEGER NOT NULL DEFAULT 0,
 status TEXT NOT NULL DEFAULT 'started'
);
CREATE INDEX IF NOT EXISTS idx_ai_date ON ai_usage(created_at);
CREATE TABLE IF NOT EXISTS bot_inbox (
 update_id INTEGER PRIMARY KEY, payload TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'pending'
);
CREATE TABLE IF NOT EXISTS state (key TEXT PRIMARY KEY, value TEXT NOT NULL);
PRAGMA user_version=1;
CREATE TABLE IF NOT EXISTS summary_parts (
 job_id INTEGER NOT NULL, chat_id INTEGER NOT NULL, level INTEGER NOT NULL,
 seq INTEGER NOT NULL, content TEXT NOT NULL,
 PRIMARY KEY(job_id,chat_id,level,seq)
);
CREATE TABLE IF NOT EXISTS summary_inputs (
 job_id INTEGER NOT NULL, chat_id INTEGER NOT NULL, message_id INTEGER NOT NULL,
 sort_date TEXT NOT NULL, message_text TEXT NOT NULL, message_link TEXT,
 PRIMARY KEY(job_id,chat_id,message_id)
);
CREATE INDEX IF NOT EXISTS idx_summary_inputs ON summary_inputs(job_id,chat_id,sort_date,message_id);
CREATE TABLE IF NOT EXISTS deletions (
 chat_id INTEGER NOT NULL, message_id INTEGER NOT NULL, observed_at TEXT NOT NULL,
 PRIMARY KEY(chat_id,message_id)
);
