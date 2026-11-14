from __future__ import annotations

from datetime import datetime, timezone

from app.db import connect

STREAM_SEEDS = [
    {
        "code": "member_profile",
        "title": "会员档案变更",
        "domain": "member",
        "entity_key_name": "member_id",
        "payload_fields": ["display_name", "tier", "region"],
        "fixture": "member_profile.json",
        "enabled": 1,
    },
    {
        "code": "store_archive",
        "title": "门店档案变更",
        "domain": "store",
        "entity_key_name": "store_id",
        "payload_fields": ["name", "status", "city"],
        "fixture": "store_archive.json",
        "enabled": 1,
    },
    {
        "code": "channel_attr",
        "title": "渠道属性变更",
        "domain": "channel",
        "entity_key_name": "channel_code",
        "payload_fields": ["label", "active"],
        "fixture": "channel_attr.json",
        "enabled": 1,
    },
]

DEFAULT_SETTINGS = {
    "apply_order": "by_seq",
    "duplicate_policy": "skip",
    "tombstone_mode": "soft_delete",
    # 租约过期后的互斥策略：allow_preempt | refuse_preempt
    "lease_preempt_policy": "refuse_preempt",
    "lease_default_ttl": "120",
}


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _ensure_column(conn, table: str, name: str, ddl: str) -> None:
    cols = {r["name"] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()}
    if name not in cols:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {name} {ddl}")


SCHEMA = """
CREATE TABLE IF NOT EXISTS project_streams(
    id INTEGER PRIMARY KEY,
    code TEXT NOT NULL UNIQUE,
    title TEXT NOT NULL,
    domain TEXT NOT NULL,
    entity_key_name TEXT NOT NULL,
    payload_fields_json TEXT NOT NULL,
    fixture TEXT NOT NULL,
    enabled INTEGER NOT NULL DEFAULT 1,
    watermark_seq INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS project_batches(
    id INTEGER PRIMARY KEY,
    batch_no TEXT NOT NULL UNIQUE,
    stream_id INTEGER NOT NULL,
    status TEXT NOT NULL,
    source_events INTEGER NOT NULL,
    applied_upsert INTEGER NOT NULL,
    applied_delete INTEGER NOT NULL,
    skipped_duplicate INTEGER NOT NULL,
    projected_alive INTEGER NOT NULL,
    projected_deleted INTEGER NOT NULL,
    max_seq INTEGER NOT NULL,
    note TEXT,
    created_at TEXT NOT NULL,
    lease_token TEXT,
    lease_generation INTEGER,
    started_at TEXT,
    finished_at TEXT,
    fence_reason TEXT,
    FOREIGN KEY(stream_id) REFERENCES project_streams(id)
);
CREATE TABLE IF NOT EXISTS projection_rows(
    id INTEGER PRIMARY KEY,
    stream_id INTEGER NOT NULL,
    entity_key TEXT NOT NULL,
    payload_json TEXT,
    applied_seq INTEGER NOT NULL,
    applied_event_id TEXT NOT NULL,
    deleted INTEGER NOT NULL DEFAULT 0,
    updated_at TEXT NOT NULL,
    UNIQUE(stream_id, entity_key),
    FOREIGN KEY(stream_id) REFERENCES project_streams(id)
);
CREATE TABLE IF NOT EXISTS applied_events(
    id INTEGER PRIMARY KEY,
    batch_id INTEGER NOT NULL,
    stream_id INTEGER NOT NULL,
    event_id TEXT NOT NULL,
    entity_key TEXT NOT NULL,
    op TEXT NOT NULL,
    seq INTEGER NOT NULL,
    event_time TEXT NOT NULL,
    result TEXT NOT NULL,
    created_at TEXT NOT NULL,
    FOREIGN KEY(batch_id) REFERENCES project_batches(id),
    FOREIGN KEY(stream_id) REFERENCES project_streams(id)
);
CREATE TABLE IF NOT EXISTS ooo_buffer(
    id INTEGER PRIMARY KEY,
    stream_id INTEGER NOT NULL,
    event_id TEXT NOT NULL,
    entity_key TEXT NOT NULL,
    op TEXT NOT NULL,
    seq INTEGER NOT NULL,
    event_time TEXT NOT NULL,
    payload_json TEXT,
    buffered_at TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'open',
    FOREIGN KEY(stream_id) REFERENCES project_streams(id)
);
CREATE TABLE IF NOT EXISTS settings(
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS stream_leases(
    id INTEGER PRIMARY KEY,
    stream_id INTEGER NOT NULL UNIQUE,
    lease_token TEXT NOT NULL,
    owner TEXT NOT NULL,
    batch_no TEXT,
    generation INTEGER NOT NULL,
    state TEXT NOT NULL,
    ttl_seconds INTEGER NOT NULL,
    acquired_at TEXT NOT NULL,
    last_heartbeat_at TEXT,
    expires_at TEXT NOT NULL,
    ended_at TEXT,
    end_reason TEXT,
    FOREIGN KEY(stream_id) REFERENCES project_streams(id)
);
CREATE TABLE IF NOT EXISTS lease_events(
    id INTEGER PRIMARY KEY,
    stream_id INTEGER NOT NULL,
    kind TEXT NOT NULL,
    batch_no TEXT,
    lease_token TEXT,
    generation INTEGER,
    actor TEXT,
    reason TEXT,
    detail TEXT,
    created_at TEXT NOT NULL,
    FOREIGN KEY(stream_id) REFERENCES project_streams(id)
);
CREATE TABLE IF NOT EXISTS batch_staging(
    batch_id INTEGER PRIMARY KEY,
    payload_json TEXT NOT NULL,
    staged_at TEXT NOT NULL,
    FOREIGN KEY(batch_id) REFERENCES project_batches(id)
);
CREATE INDEX IF NOT EXISTS idx_lease_events_stream ON lease_events(stream_id, id);
CREATE INDEX IF NOT EXISTS idx_batches_stream_status ON project_batches(stream_id, status);
"""


def init_db() -> None:
    import json

    conn = connect()
    try:
        conn.executescript(SCHEMA)
        # Older volumes predate lease columns on project_batches.
        _ensure_column(conn, "project_batches", "lease_token", "TEXT")
        _ensure_column(conn, "project_batches", "lease_generation", "INTEGER")
        _ensure_column(conn, "project_batches", "started_at", "TEXT")
        _ensure_column(conn, "project_batches", "finished_at", "TEXT")
        _ensure_column(conn, "project_batches", "fence_reason", "TEXT")

        if conn.execute("SELECT COUNT(*) c FROM project_streams").fetchone()["c"] == 0:
            now = _now()
            for s in STREAM_SEEDS:
                conn.execute(
                    """
                    INSERT INTO project_streams(
                        code, title, domain, entity_key_name, payload_fields_json,
                        fixture, enabled, watermark_seq, created_at
                    ) VALUES (?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        s["code"],
                        s["title"],
                        s["domain"],
                        s["entity_key_name"],
                        json.dumps(s["payload_fields"], ensure_ascii=False),
                        s["fixture"],
                        s["enabled"],
                        0,
                        now,
                    ),
                )
        for key, value in DEFAULT_SETTINGS.items():
            conn.execute(
                "INSERT OR IGNORE INTO settings(key, value) VALUES (?, ?)", (key, value)
            )
        conn.commit()
    finally:
        conn.close()
