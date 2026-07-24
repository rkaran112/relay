"""Phase 6: the control-plane API."""

from __future__ import annotations

from datetime import timedelta

from fastapi.testclient import TestClient
from psycopg import Connection
from psycopg.rows import TupleRow

from relay.api import create_app
from relay.models import Job
from relay.worker import Worker


def test_healthz(dsn: str) -> None:
    with TestClient(create_app(dsn)) as client:
        r = client.get("/healthz")
        assert r.status_code == 200
        assert r.json() == {"status": "ok"}


def test_enqueue_is_idempotent_and_job_is_readable(dsn: str) -> None:
    with TestClient(create_app(dsn)) as client:
        body = {"payload": {"to": "a@b.com"}, "idempotency_key": "k1"}
        first = client.post("/queues/emails/jobs", json=body)
        assert first.status_code == 201
        job_id = first.json()["id"]

        again = client.post("/queues/emails/jobs", json=body)
        assert again.json()["id"] == job_id  # deduped

        got = client.get(f"/jobs/{job_id}")
        assert got.status_code == 200
        assert got.json()["queue"] == "emails"
        assert got.json()["state"] == "pending"

        assert client.get("/jobs/99999999").status_code == 404


def test_stats_reports_depth(dsn: str) -> None:
    with TestClient(create_app(dsn)) as client:
        for i in range(3):
            client.post("/queues/q/jobs", json={"payload": {"i": i}})
        stats = client.get("/queues/q/stats").json()
        assert stats["queue"] == "q"
        assert stats["pending"] == 3
        assert stats["succeeded"] == 0


def test_metrics_endpoint_exposes_queue_depth(dsn: str) -> None:
    with TestClient(create_app(dsn)) as client:
        client.post("/queues/m/jobs", json={"payload": {}})
        body = client.get("/metrics").text
        assert 'relay_jobs{queue="m",state="pending"} 1.0' in body


# Module-level flag simulating a transient downstream outage that a replay recovers from.
_downstream_down = {"v": True}


def _flaky(conn: Connection[TupleRow], job: Job) -> None:
    if _downstream_down["v"]:
        raise RuntimeError("downstream unavailable")
    conn.execute("INSERT INTO side_effects (job_id, value) VALUES (%s, 'ok')", (job.id,))


def test_dead_letter_can_be_replayed_and_succeeds(dsn: str) -> None:
    _downstream_down["v"] = True
    with TestClient(create_app(dsn)) as client:
        created = client.post(
            "/queues/dlq_q/jobs", json={"payload": {"n": 1}, "max_attempts": 1}
        )
        job_id = created.json()["id"]

    # First run: downstream is down, the job exhausts its one attempt and dead-letters.
    Worker(dsn, "dlq_q", _flaky, backoff_base=0.0, lease=timedelta(seconds=30)).run_until_empty()

    with TestClient(create_app(dsn)) as client:
        dlq = client.get("/queues/dlq_q/dlq").json()
        assert len(dlq) == 1
        assert dlq[0]["job_id"] == job_id

        replayed = client.post(f"/queues/dlq_q/dlq/{dlq[0]['id']}/replay")
        assert replayed.status_code == 200
        new_id = replayed.json()["id"]
        assert new_id != job_id

    # Downstream recovers; the replayed job now succeeds.
    _downstream_down["v"] = False
    Worker(dsn, "dlq_q", _flaky, backoff_base=0.0, lease=timedelta(seconds=30)).run_until_empty()

    with TestClient(create_app(dsn)) as client:
        assert client.get(f"/jobs/{new_id}").json()["state"] == "succeeded"
        # replaying the same dead letter again is rejected
        stale = client.get("/queues/dlq_q/dlq").json()
        assert client.post(f"/queues/dlq_q/dlq/{stale[0]['id']}/replay").status_code == 404
