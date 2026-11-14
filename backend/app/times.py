from __future__ import annotations

from datetime import datetime, timedelta, timezone

ISO_Z = "%Y-%m-%dT%H:%M:%S.%f"


def now_dt() -> datetime:
    return datetime.now(timezone.utc)


def iso(dt: datetime) -> str:
    """UTC timestamp with millisecond precision, e.g. 2026-03-01T08:00:00.123Z.

    Lease expiry needs sub-second precision; the older second-based columns are
    still written in a compatible (superset) format.
    """
    return dt.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def now_iso() -> str:
    return iso(now_dt())


def parse_iso(value: str) -> datetime:
    text = str(value).strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    dt = datetime.fromisoformat(text)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def add_seconds(dt: datetime, seconds: int) -> datetime:
    return dt + timedelta(seconds=int(seconds))
