#!/usr/bin/env bash
# Apply migrations, then hand off to the container command. depends_on:
# service_healthy in compose guarantees Postgres is accepting connections before
# we get here, so a bare `relay migrate` is safe.
set -euo pipefail

echo "relay: applying migrations..."
relay migrate

echo "relay: migrations applied, starting: $*"
exec "$@"
