"""End-to-end API tests for stream leases: expiry, heartbeat, preemption,
reclaim, and fenced commits."""

from __future__ import annotations

import time


def _stream(c, code="member_profile") -> int:
    for s in c.get("/api/streams").json():
        if s["code"] == code:
            return s["id"]
    raise AssertionError(code)


def _set_policy(c, policy, ttl=2):
    r = c.put("/api/leases/policy", json={"policy": policy, "default_ttl": ttl})
    assert r.status_code == 200, r.text


def _expire(c, sid):
    lease = c.get(f"/api/leases/{sid}").json()
    left = max(0.0, lease["remaining_seconds"])
    time.sleep(left + 0.35)


def test_run_acquires_and_releases_lease(client):
    sid = _stream(client)
    r = client.post("/api/project", json={"stream_id": sid})
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["status"] == "completed"
    assert body["lease_token"]

    lease = client.get(f"/api/leases/{sid}").json()
    assert lease["state"] == "released"
    assert lease["held"] is False
    assert lease["counters"]["acquire_count"] == 1
    assert lease["counters"]["complete_count"] == 1
    assert lease["last_completed_batch"]["batch_no"] == body["batch_no"]
    assert lease["watermark_seq"] == body["max_seq"]


def test_second_run_rejected_while_lease_held(client):
    sid = _stream(client)
    acquired = client.post(
        "/api/leases/acquire", json={"stream_id": sid, "owner": "hold-A", "ttl_seconds": 30}
    )
    assert acquired.status_code == 200, acquired.text
    token = acquired.json()["lease_token"]

    r = client.post("/api/project", json={"stream_id": sid, "note": "第二趟"})
    assert r.status_code == 409
    detail = r.json()["detail"]
    assert detail["code"] == "LEASE_HELD"
    assert detail["lease"]["lease_token"] == token

    lease = client.get(f"/api/leases/{sid}").json()
    assert lease["held"] is True
    assert lease["counters"]["reject_count"] == 1
    # Rejection must not create a batch nor move the projection.
    assert client.get("/api/batches").json() == []
    assert client.get(f"/api/projections?stream_id={sid}").json() == []

    other = _stream(client, "store_archive")
    ok = client.post("/api/project", json={"stream_id": other})
    assert ok.status_code == 200, ok.text


def test_heartbeat_extends_and_renews(client):
    sid = _stream(client)
    acquired = client.post(
        "/api/leases/acquire", json={"stream_id": sid, "ttl_seconds": 4}
    ).json()
    token = acquired["lease_token"]
    first_expiry = acquired["expires_at"]

    time.sleep(1.0)
    beat = client.post(
        "/api/leases/heartbeat", json={"stream_id": sid, "lease_token": token, "ttl_seconds": 6}
    )
    assert beat.status_code == 200, beat.text
    renewed = beat.json()
    assert renewed["expires_at"] > first_expiry
    assert renewed["counters"]["heartbeat_count"] == 1

    bad = client.post(
        "/api/leases/heartbeat", json={"stream_id": sid, "lease_token": "L-forged"}
    )
    assert bad.status_code == 409
    assert bad.json()["detail"]["code"] == "LEASE_TOKEN_MISMATCH"


def test_expiry_refuse_policy_keeps_blocking_until_reclaim(client):
    sid = _stream(client)
    _set_policy(client, "refuse_preempt", ttl=2)
    acquired = client.post(
        "/api/leases/acquire", json={"stream_id": sid, "owner": "crashed", "ttl_seconds": 2}
    ).json()
    token = acquired["lease_token"]
    _expire(client, sid)

    lease = client.get(f"/api/leases/{sid}").json()
    assert lease["state"] == "expired" and lease["expired"] is True

    blocked = client.post("/api/project", json={"stream_id": sid})
    assert blocked.status_code == 409
    assert blocked.json()["detail"]["code"] == "LEASE_EXPIRED_REFUSED"

    # Heartbeat on an expired lease is refused too (cannot silently resurrect).
    beat = client.post(
        "/api/leases/heartbeat", json={"stream_id": sid, "lease_token": token}
    )
    assert beat.status_code == 409
    assert beat.json()["detail"]["code"] == "LEASE_EXPIRED"

    reclaimed = client.post("/api/leases/reclaim", json={"stream_id": sid, "reason": "worker dead"})
    assert reclaimed.status_code == 200, reclaimed.text
    assert reclaimed.json()["state"] == "reclaimed"
    assert reclaimed.json()["counters"]["reclaim_count"] == 1

    ran = client.post("/api/project", json={"stream_id": sid})
    assert ran.status_code == 200, ran.text
    assert ran.json()["status"] == "completed"

    counters = client.get(f"/api/leases/{sid}").json()["counters"]
    # One refusal for the second run, one for the heartbeat on an expired lease.
    assert counters["reject_count"] == 2
    assert counters["reclaim_count"] == 1
    assert counters["acquire_count"] == 2


def test_expired_lease_cannot_be_reclaimed_early(client):
    sid = _stream(client)
    client.post("/api/leases/acquire", json={"stream_id": sid, "ttl_seconds": 30})
    r = client.post("/api/leases/reclaim", json={"stream_id": sid})
    assert r.status_code == 409
    assert r.json()["detail"]["code"] == "LEASE_NOT_EXPIRED"


def _start_crashed_run(c, sid, ttl=2):
    started = c.post(
        "/api/runs/start",
        json={"stream_id": sid, "owner": "crashed-worker", "ttl_seconds": ttl},
    )
    assert started.status_code == 200, started.text
    body = started.json()
    return body["batch"]["id"], body["batch"]["batch_no"], body["lease_token"]


def test_crashed_batch_blocks_stream_until_lease_expires(client):
    sid = _stream(client)
    _set_policy(client, "refuse_preempt", ttl=2)
    batch_id, batch_no, token = _start_crashed_run(client, sid, ttl=2)

    # While the lease is alive the second run is refused outright.
    blocked = client.post("/api/project", json={"stream_id": sid})
    assert blocked.status_code == 409
    assert blocked.json()["detail"]["code"] == "LEASE_HELD"

    batch = client.get(f"/api/runs/{batch_id}").json()
    assert batch["status"] == "pending"
    assert batch["staged"] is True

    _expire(client, sid)
    # Refuse policy: still blocked even though the lease is expired.
    still = client.post("/api/project", json={"stream_id": sid})
    assert still.status_code == 409
    assert still.json()["detail"]["code"] == "LEASE_EXPIRED_REFUSED"

    # Reclaim marks the crashed batch failed and frees the stream.
    reclaimed = client.post("/api/leases/reclaim", json={"stream_id": sid})
    assert reclaimed.status_code == 200, reclaimed.text
    failed = client.get(f"/api/runs/{batch_id}").json()
    assert failed["status"] == "failed"
    assert "回收" in failed["fence_reason"]

    # The old worker's commit is fenced and must not touch the projection.
    late = client.post(
        "/api/runs/commit", json={"batch_id": batch_id, "lease_token": token}
    )
    assert late.status_code == 409
    assert late.json()["detail"]["code"] == "FENCED"
    assert client.get(f"/api/projections?stream_id={sid}").json() == []

    ran = client.post("/api/project", json={"stream_id": sid})
    assert ran.status_code == 200, ran.text

    failed = client.get(f"/api/runs/{batch_id}").json()
    assert failed["status"] == "failed"
    counters = client.get(f"/api/leases/{sid}").json()["counters"]
    assert counters["fence_count"] == 1
    assert counters["reclaim_count"] == 1


def test_allow_preempt_policy_invalidates_old_batch(client):
    sid = _stream(client)
    _set_policy(client, "allow_preempt", ttl=2)
    batch_id, batch_no, token = _start_crashed_run(client, sid, ttl=2)
    _expire(client, sid)

    # Second run takes over and voids the crashed batch.
    ran = client.post("/api/project", json={"stream_id": sid})
    assert ran.status_code == 200, ran.text
    new_batch = ran.json()

    old = client.get(f"/api/runs/{batch_id}").json()
    assert old["status"] == "preempted"
    assert "抢占" in old["fence_reason"]

    late = client.post(
        "/api/runs/commit", json={"batch_id": batch_id, "lease_token": token}
    )
    assert late.status_code == 409
    assert late.json()["detail"]["code"] == "FENCED"

    # Projection reflects the new batch, not the fenced old one.
    projections = client.get(f"/api/projections?stream_id={sid}").json()
    assert projections
    assert {p["applied_event_id"] for p in projections} <= {
        e["event_id"] for e in client.get(f"/api/events?batch_id={new_batch['id']}").json()
    }

    events = client.get(f"/api/leases/events?stream_id={sid}").json()
    kinds = [e["kind"] for e in events]
    assert "preempt" in kinds
    preempt = next(e for e in events if e["kind"] == "preempt")
    assert preempt["reason"] == "lease_expired_preempted"
    assert batch_no in preempt["detail"]

    counters = client.get(f"/api/leases/{sid}").json()["counters"]
    assert counters["preempt_count"] == 1
    assert counters["fence_count"] == 1


def test_expired_lease_commit_is_fenced_without_preemption(client):
    sid = _stream(client)
    _set_policy(client, "refuse_preempt", ttl=2)
    batch_id, _, token = _start_crashed_run(client, sid, ttl=2)
    _expire(client, sid)

    late = client.post(
        "/api/runs/commit", json={"batch_id": batch_id, "lease_token": token}
    )
    assert late.status_code == 409
    assert late.json()["detail"]["code"] == "FENCED"
    assert "过期" in late.json()["detail"]["fence_reason"]

    failed = client.get(f"/api/runs/{batch_id}").json()
    assert failed["status"] == "fenced"
    assert client.get(f"/api/projections?stream_id={sid}").json() == []
    assert client.get("/api/streams").json()[sid - 1]["watermark_seq"] == 0


def test_explicit_release_fails_pending_batch(client):
    sid = _stream(client)
    batch_id, _, token = _start_crashed_run(client, sid, ttl=60)
    released = client.post(
        "/api/leases/release", json={"stream_id": sid, "lease_token": token, "reason": "abort"}
    )
    assert released.status_code == 200, released.text
    assert released.json()["counters"]["release_count"] == 1

    batch = client.get(f"/api/runs/{batch_id}").json()
    assert batch["status"] == "failed"

    late = client.post(
        "/api/runs/commit", json={"batch_id": batch_id, "lease_token": token}
    )
    assert late.status_code == 409
    assert late.json()["detail"]["code"] == "FENCED"

    again = client.post("/api/project", json={"stream_id": sid})
    assert again.status_code == 200, again.text


def test_audit_cross_checks_projection_watermark_and_lease(client):
    sid = _stream(client)
    _set_policy(client, "allow_preempt", ttl=2)
    ran = client.post("/api/project", json={"stream_id": sid})
    assert ran.status_code == 200
    body = ran.json()

    audit = client.get("/api/leases/audit").json()
    mine = [c for c in audit["checks"] if c["stream_id"] == sid]
    assert mine and all(c["ok"] for c in mine), mine

    projections = client.get(f"/api/projections?stream_id={sid}").json()
    streams = {s["id"]: s for s in client.get("/api/streams").json()}
    assert streams[sid]["watermark_seq"] == body["max_seq"]
    assert sum(1 for p in projections if not p["deleted"]) == body["projected_alive"]
    assert sum(1 for p in projections if p["deleted"]) == body["projected_deleted"]


def test_heartbeat_keeps_crashed_batch_committable(client):
    sid = _stream(client)
    batch_id, _, token = _start_crashed_run(client, sid, ttl=2)
    # Renew past the original expiry, then commit: the batch is still fenced-in.
    for _ in range(3):
        time.sleep(1.0)
        beat = client.post(
            "/api/leases/heartbeat",
            json={"stream_id": sid, "lease_token": token, "ttl_seconds": 4},
        )
        assert beat.status_code == 200, beat.text

    done = client.post(
        "/api/runs/commit", json={"batch_id": batch_id, "lease_token": token}
    )
    assert done.status_code == 200, done.text
    assert done.json()["status"] == "completed"

    projections = client.get(f"/api/projections?stream_id={sid}").json()
    assert projections
    watermark = next(s for s in client.get("/api/streams").json() if s["id"] == sid)
    assert watermark["watermark_seq"] == done.json()["max_seq"]
    assert client.get(f"/api/runs/{batch_id}").json()["status"] == "completed"


def test_old_batch_commit_after_reclaim_is_fenced(client):
    sid = _stream(client)
    _set_policy(client, "refuse_preempt", ttl=2)
    batch_id, _, token = _start_crashed_run(client, sid, ttl=2)
    _expire(client, sid)
    # Old worker resurrects and tries to commit *before* the reclaim.
    pre = client.post("/api/runs/commit", json={"batch_id": batch_id, "lease_token": token})
    assert pre.status_code == 409 and pre.json()["detail"]["code"] == "FENCED"

    reclaimed = client.post("/api/leases/reclaim", json={"stream_id": sid})
    assert reclaimed.status_code == 200, reclaimed.text
    batch = client.get(f"/api/runs/{batch_id}").json()
    assert batch["status"] == "failed"
    # Both reasons survive: the earlier fence and the later reclaim.
    assert "过期" in batch["fence_reason"] and "回收" in batch["fence_reason"]
    assert client.get(f"/api/projections?stream_id={sid}").json() == []

    late = client.post("/api/runs/commit", json={"batch_id": batch_id, "lease_token": token})
    assert late.status_code == 409 and late.json()["detail"]["code"] == "FENCED"

    audit = client.get("/api/leases/audit").json()
    assert [c for c in audit["checks"] if c["stream_id"] == sid and not c["ok"]] == []


def test_release_with_wrong_token_is_rejected(client):
    sid = _stream(client)
    client.post("/api/leases/acquire", json={"stream_id": sid, "ttl_seconds": 30})
    r = client.post("/api/leases/release", json={"stream_id": sid, "lease_token": "nope"})
    assert r.status_code == 409
    assert r.json()["detail"]["code"] == "LEASE_TOKEN_MISMATCH"
    lease = client.get(f"/api/leases/{sid}").json()
    assert lease["held"] is True
