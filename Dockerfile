# relay application image, built with uv. On boot the entrypoint applies Alembic
# migrations, then runs the given command. Kept deliberately small: Postgres and
# Redis are separate services in docker-compose.yml.
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# uv, copied from its published image (no curl|sh bootstrap needed).
COPY --from=ghcr.io/astral-sh/uv:0.5 /uv /usr/local/bin/uv

# Install deps + the app itself (hatchling needs the source present).
COPY . .
RUN uv sync --frozen --no-dev

ENV PATH="/app/.venv/bin:$PATH"

RUN chmod +x /app/docker/entrypoint.sh

ENTRYPOINT ["/app/docker/entrypoint.sh"]
# Default: run the whole system (API + dashboard + workers + reaper + publisher). The
# entrypoint applies migrations first, then execs this. Binds 0.0.0.0 and reads $PORT.
# docker-compose and other hosts can override this command.
CMD ["relay", "demo", "--host", "0.0.0.0", "--workers", "2"]
