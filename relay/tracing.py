"""OpenTelemetry wiring.

The interesting bit is that a trace survives the queue hop. The producer's enqueue
span writes its W3C ``traceparent`` onto the job row; when a worker later handles
that job it extracts the traceparent and starts its handle span as a *child* of the
producer's context. So one trace spans producer -> queue -> handler, which most
queue integrations lose at the boundary.

Tracing is opt-in: with no provider configured, ``get_tracer`` returns a no-op and
``current_traceparent`` returns None, so the queue runs untraced at zero cost.
"""

from __future__ import annotations

import os
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from typing import Any

from opentelemetry import propagate, trace
from opentelemetry.trace import Tracer

_configured = False


def configure_tracing(service_name: str = "relay", endpoint: str | None = None) -> None:
    """Install an OTLP-exporting tracer provider. Idempotent. ``endpoint`` defaults
    to ``OTEL_EXPORTER_OTLP_ENDPOINT`` (e.g. http://localhost:4318 for Jaeger)."""
    global _configured
    if _configured:
        return
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor

    provider = TracerProvider(resource=Resource.create({"service.name": service_name}))
    endpoint = endpoint or os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT")
    if endpoint:
        from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter

        provider.add_span_processor(
            BatchSpanProcessor(OTLPSpanExporter(endpoint=f"{endpoint.rstrip('/')}/v1/traces"))
        )
    trace.set_tracer_provider(provider)
    _configured = True


def tracer() -> Tracer:
    return trace.get_tracer("relay")


def current_traceparent() -> str | None:
    """W3C traceparent for the active span, or None if nothing is being recorded."""
    carrier: dict[str, str] = {}
    propagate.inject(carrier)
    return carrier.get("traceparent")


@contextmanager
def handle_span(
    name: str, traceparent: str | None, attributes: Mapping[str, Any] | None = None
) -> Iterator[None]:
    """Start ``name`` as a child of the remote context carried by ``traceparent``."""
    parent = propagate.extract({"traceparent": traceparent}) if traceparent else None
    with tracer().start_as_current_span(name, context=parent, attributes=dict(attributes or {})):
        yield
