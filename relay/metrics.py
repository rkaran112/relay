"""Prometheus metrics.

The control plane can only see what's in the database, so these are the queue-state
metrics — depth by state, oldest pending age, dlq backlog — computed fresh on each
scrape from a small, indexed set of aggregate queries. Worker-side timings (handler
duration, throughput) are recorded in-process by the worker and are out of scope for
the API scrape; see RESULTS.md.
"""

from __future__ import annotations

from prometheus_client import CollectorRegistry, Gauge, generate_latest
from psycopg import Connection
from psycopg.rows import TupleRow


def render_metrics(conn: Connection[TupleRow]) -> bytes:
    registry = CollectorRegistry()
    depth = Gauge(
        "relay_jobs",
        "Number of jobs by queue and state",
        ["queue", "state"],
        registry=registry,
    )
    oldest = Gauge(
        "relay_oldest_pending_age_seconds",
        "Age of the oldest pending job, by queue",
        ["queue"],
        registry=registry,
    )
    dlq = Gauge(
        "relay_dead_letters",
        "Unreplayed dead letters, by queue",
        ["queue"],
        registry=registry,
    )

    for queue, state, n in conn.execute(
        "SELECT queue, state, count(*) FROM jobs GROUP BY queue, state"
    ).fetchall():
        depth.labels(queue=queue, state=state).set(n)

    for queue, age in conn.execute(
        "SELECT queue, EXTRACT(EPOCH FROM (now() - min(run_at))) "
        "FROM jobs WHERE state = 'pending' GROUP BY queue"
    ).fetchall():
        oldest.labels(queue=queue).set(float(age) if age is not None else 0.0)

    for queue, n in conn.execute(
        "SELECT queue, count(*) FROM dead_letters WHERE replayed_at IS NULL GROUP BY queue"
    ).fetchall():
        dlq.labels(queue=queue).set(n)

    return generate_latest(registry)
