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
# Placeholder workload for Phase 0: stay alive after migrating. Replaced by the
# worker / publisher / api commands in later phases.
CMD ["sleep", "infinity"]
