"""The control-plane HTTP API (FastAPI) and the dashboard it serves.

Endpoints:
  POST /queues/{q}/jobs            enqueue (idempotent on idempotency_key)
  GET  /jobs/{id}                  job status
  GET  /queues/{q}/stats           depth by state, oldest pending age, dlq size
  GET  /queues/{q}/dlq             dead letters
  POST /queues/{q}/dlq/{id}/replay re-enqueue a dead letter
  GET  /metrics                    Prometheus exposition (see relay.metrics)
  GET  /healthz                    liveness + DB check
  GET  /                           the dashboard
"""

from __future__ import annotations

import contextlib
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any, cast

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, HTMLResponse, Response
from prometheus_client import CONTENT_TYPE_LATEST
from psycopg_pool import ConnectionPool
from pydantic import BaseModel, Field

from . import admin
from .config import get_settings
from .metrics import render_metrics
from .models import Job
from .queue import enqueue

_WEB_DIR = Path(__file__).resolve().parent / "web"


class EnqueueRequest(BaseModel):
    payload: dict[str, Any] = Field(default_factory=dict)
    idempotency_key: str | None = None
    max_attempts: int = 5
    priority: int = 0


class IdResponse(BaseModel):
    id: int


def _pool(request: Request) -> ConnectionPool:
    return cast(ConnectionPool, request.app.state.pool)


def _job_dict(job: Job) -> dict[str, Any]:
    return {
        "id": job.id,
        "queue": job.queue,
        "payload": job.payload,
        "state": job.state.value,
        "priority": job.priority,
        "attempts": job.attempts,
        "max_attempts": job.max_attempts,
        "run_at": job.run_at.isoformat(),
        "lease_owner": str(job.lease_owner) if job.lease_owner is not None else None,
        "lease_token": job.lease_token,
        "lease_expires_at": (
            job.lease_expires_at.isoformat() if job.lease_expires_at is not None else None
        ),
        "idempotency_key": job.idempotency_key,
        "created_at": job.created_at.isoformat(),
    }


def create_app(dsn: str | None = None) -> FastAPI:
    conninfo = dsn or get_settings().psycopg_dsn

    @contextlib.asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        pool = ConnectionPool(conninfo, min_size=1, max_size=10, open=True)
        app.state.pool = pool
        try:
            yield
        finally:
            pool.close()

    app = FastAPI(title="relay control plane", version="0.1.0", lifespan=lifespan)

    @app.get("/healthz")
    def healthz(request: Request) -> dict[str, str]:
        with _pool(request).connection() as conn:
            conn.execute("SELECT 1")
        return {"status": "ok"}

    @app.post("/queues/{queue}/jobs", response_model=IdResponse, status_code=201)
    def create_job(queue: str, body: EnqueueRequest, request: Request) -> IdResponse:
        with _pool(request).connection() as conn:
            job_id = enqueue(
                conn,
                queue,
                body.payload,
                idempotency_key=body.idempotency_key,
                max_attempts=body.max_attempts,
                priority=body.priority,
            )
        return IdResponse(id=job_id)

    @app.get("/jobs/{job_id}")
    def read_job(job_id: int, request: Request) -> dict[str, Any]:
        with _pool(request).connection() as conn:
            job = admin.get_job(conn, job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="job not found")
        return _job_dict(job)

    @app.get("/queues/{queue}/stats")
    def read_stats(queue: str, request: Request) -> dict[str, Any]:
        with _pool(request).connection() as conn:
            return admin.queue_stats(conn, queue)

    @app.get("/queues/{queue}/dlq")
    def read_dlq(queue: str, request: Request, limit: int = 50) -> list[dict[str, Any]]:
        with _pool(request).connection() as conn:
            return admin.list_dead_letters(conn, queue, limit=limit)

    @app.post("/queues/{queue}/dlq/{dlq_id}/replay", response_model=IdResponse)
    def replay(queue: str, dlq_id: int, request: Request) -> IdResponse:
        with _pool(request).connection() as conn:
            new_id = admin.replay_dead_letter(conn, queue, dlq_id)
        if new_id is None:
            raise HTTPException(status_code=404, detail="dead letter not found or already replayed")
        return IdResponse(id=new_id)

    @app.get("/metrics")
    def metrics(request: Request) -> Response:
        with _pool(request).connection() as conn:
            body = render_metrics(conn)
        return Response(content=body, media_type=CONTENT_TYPE_LATEST)

    @app.get("/", include_in_schema=False)
    def dashboard() -> Response:
        index = _WEB_DIR / "index.html"
        if index.exists():
            return FileResponse(index)
        return HTMLResponse("<h1>relay</h1><p>dashboard not built yet</p>")

    return app
