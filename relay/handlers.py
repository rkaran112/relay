"""Built-in job handlers.

``side_effect_handler`` is the canonical exactly-once effect used by the dashboard
demo and the chaos harness: it writes one ``side_effects`` row and emits an outbox
event, both inside the framework-provided transaction. Its behaviour is driven by
the job payload so the same handler can simulate slow work and induced failures:

    {"work_ms": 40, "fail": false, "fail_rate": 0.0, "error": "boom"}
"""

from __future__ import annotations

import random
import time

from psycopg import Connection
from psycopg.rows import TupleRow

from .models import Job
from .outbox import emit


def side_effect_handler(conn: Connection[TupleRow], job: Job) -> None:
    payload = job.payload
    work_ms = int(payload.get("work_ms", 0))
    if work_ms:
        time.sleep(work_ms / 1000.0)

    fail_rate = float(payload.get("fail_rate", 0.0))
    if payload.get("fail") or (fail_rate and random.random() < fail_rate):
        raise RuntimeError(str(payload.get("error", "induced failure")))

    conn.execute(
        "INSERT INTO side_effects (job_id, value) VALUES (%s, %s)",
        (job.id, str(payload)),
    )
    emit(conn, aggregate_id=str(job.id), event_type="job.succeeded", payload={"queue": job.queue})
