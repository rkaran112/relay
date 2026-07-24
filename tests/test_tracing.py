"""Phase 7: the trace survives the queue hop."""

from __future__ import annotations

from datetime import timedelta

from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from psycopg import Connection
from psycopg.rows import TupleRow

from relay.db import connect
from relay.models import Job
from relay.worker import Worker


def _ok(conn: Connection[TupleRow], job: Job) -> None:
    conn.execute("INSERT INTO side_effects (job_id, value) VALUES (%s, 'ok')", (job.id,))


def test_trace_spans_producer_through_handler(dsn: str) -> None:
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    trace.set_tracer_provider(provider)  # replaces the default no-op (once per process)

    # Import enqueue lazily so it binds the tracer via the now-installed provider.
    from relay.queue import enqueue

    tracer = trace.get_tracer("test")
    with connect(dsn, autocommit=True) as conn:
        with tracer.start_as_current_span("producer.request"):
            job_id = enqueue(conn, "traced", {"work_ms": 0})
        stored = conn.execute(
            "SELECT traceparent FROM jobs WHERE id = %s", (job_id,)
        ).fetchone()
        assert stored is not None and stored[0] is not None  # traceparent persisted on the row

    Worker(dsn, "traced", _ok, lease=timedelta(seconds=30)).run_until_empty()

    spans = exporter.get_finished_spans()
    names = {s.name for s in spans}
    assert {"relay.enqueue", "relay.handle"} <= names

    # producer.request, relay.enqueue and relay.handle all share one trace id.
    traced = [s for s in spans if s.name in {"producer.request", "relay.enqueue", "relay.handle"}]
    trace_ids = {s.context.trace_id for s in traced if s.context is not None}
    assert len(trace_ids) == 1
