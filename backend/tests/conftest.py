from __future__ import annotations

import os
import tempfile

# Must be set before app.config is imported anywhere.
os.environ["DATA_DIR"] = tempfile.mkdtemp(prefix="changecast-test-")

import pytest  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

from app.db import DB_PATH  # noqa: E402
from app.main import app  # noqa: E402
from app.seed import init_db  # noqa: E402


@pytest.fixture()
def client():
    if DB_PATH.exists():
        DB_PATH.unlink()
    init_db()
    with TestClient(app) as c:
        assert c.get("/api/health").json()["ok"]
        yield c
