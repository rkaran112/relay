# Deploying relay

relay is a queue: its workers, reaper and publisher are long-lived processes, so it
needs a host that runs long-lived processes — not a pure serverless platform. Render
and Fly.io both fit and both have a free tier. Render is the one-click path.

## Why not Vercel

Vercel runs request-scoped serverless functions. It can serve the API and dashboard,
but it cannot run `run_forever` worker/reaper/publisher loops, so enqueued jobs would
never be processed. Use Render/Fly for the whole system, or (if Vercel is mandatory)
split the web tier onto Vercel and run the workers on a small always-on box.

---

## Render (recommended, one click)

Everything is described in [`render.yaml`](render.yaml): a managed Postgres, a
key-value (Redis) store, and one web service that runs `relay demo` (API + dashboard +
workers + reaper + publisher). Migrations apply on boot.

1. **Push to GitHub** (Render deploys from a repo):
   ```bash
   git remote add origin https://github.com/<you>/relay.git
   git push -u origin master
   ```
2. **Render → New → Blueprint**, pick the repo. Render reads `render.yaml` and creates
   all three resources. Click **Apply**.
3. Wait for the build. The web service's URL (`https://relay-XXXX.onrender.com`) serves
   the dashboard; `GET /healthz` is the health check.

Notes:
- **Free web services sleep after ~15 min idle** and cold-start (~30 s) on the next
  request; workers pause while asleep. Fine for a portfolio demo. For always-on
  processing, take the web service off free, or add a separate `type: worker` service
  running the workers (below).
- **Split web and workers (for scale):** add a second service to `render.yaml`:
  ```yaml
  - type: worker
    name: relay-workers
    runtime: docker
    dockerfilePath: ./Dockerfile
    dockerContext: .
    dockerCommand: sh -c "relay reaper & relay publisher & relay worker --queue default"
    envVars:
      - { key: DATABASE_URL, fromDatabase: { name: relay-db, property: connectionString } }
      - { key: REDIS_URL, fromService: { type: keyvalue, name: relay-redis, property: connectionString } }
  ```
  and change the web service's `dockerCommand` to `relay serve --host 0.0.0.0`.
- **If your Render account predates "Key Value":** change `type: keyvalue` to
  `type: redis`. Redis is optional anyway — without it the queue and dashboard work
  fully; only the outbox→stream publisher is idle (it fails quietly).

---

## Fly.io (alternative)

Fly runs the Docker image directly.

```bash
fly launch --no-deploy               # detects the Dockerfile; edit fly.toml as below
fly postgres create                  # managed Postgres; attach it:
fly postgres attach <pg-app-name>    # sets DATABASE_URL secret automatically
fly deploy
```

In `fly.toml`, run the all-in-one process and expose the app port:

```toml
[processes]
  app = "relay demo --host 0.0.0.0 --port 8080"

[http_service]
  internal_port = 8080
  force_https = true

[[http_service.checks]]
  path = "/healthz"
```

Redis (optional) via Upstash: `fly redis create`, then
`fly secrets set REDIS_URL=<upstash-url>`.

---

## Environment variables

| Var | Required | Notes |
|---|---|---|
| `DATABASE_URL` | yes | Any Postgres scheme works — `postgres://`, `postgresql://`, or `postgresql+psycopg://`. relay normalizes it. For an **external** DB (Neon, etc.) append `?sslmode=require`. |
| `REDIS_URL` | no | Only the outbox publisher uses it. Absent → publisher idles, everything else works. |
| `PORT` | auto | Set by the platform; `relay serve`/`demo` read it. |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | no | Point at an OTLP collector (Jaeger) to export traces. |

The Docker entrypoint runs `relay migrate` before starting, so the schema is created
on first boot. No manual migration step.
