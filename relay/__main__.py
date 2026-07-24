"""relay command-line entry point.

  relay migrate                 apply the schema
  relay version                 print the version
  relay serve   [--host --port] run the control-plane API + dashboard
  relay worker  [--queue ...]   run a worker (side_effect_handler)
  relay reaper  [--interval]    run the visibility-timeout reaper
  relay publisher [--stream]    run the outbox -> Redis publisher
"""

from __future__ import annotations

import argparse
from datetime import timedelta

from . import __version__, migrations


def _serve(args: argparse.Namespace) -> None:
    import uvicorn

    from .api import create_app

    uvicorn.run(create_app(), host=args.host, port=args.port)


def _worker(args: argparse.Namespace) -> None:
    from .config import get_settings
    from .handlers import side_effect_handler
    from .worker import Worker

    worker = Worker(
        get_settings().psycopg_dsn,
        args.queue,
        side_effect_handler,
        batch=args.batch,
        lease=timedelta(seconds=args.lease),
    )
    print(f"relay worker on '{args.queue}' (owner={worker.owner})", flush=True)
    try:
        worker.run_forever()
    except KeyboardInterrupt:
        worker.stop()


def _reaper(args: argparse.Namespace) -> None:
    from .config import get_settings
    from .reaper import Reaper

    reaper = Reaper(get_settings().psycopg_dsn, interval=args.interval)
    print("relay reaper running", flush=True)
    try:
        reaper.run_forever()
    except KeyboardInterrupt:
        reaper.stop()


def _publisher(args: argparse.Namespace) -> None:
    from .config import get_settings
    from .publisher import Publisher

    settings = get_settings()
    publisher = Publisher(settings.psycopg_dsn, settings.redis_url, stream=args.stream)
    print(f"relay publisher -> {args.stream}", flush=True)
    try:
        publisher.run_forever()
    except KeyboardInterrupt:
        publisher.stop()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="relay", description="relay task queue")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("migrate", help="apply database migrations")
    sub.add_parser("version", help="print the relay version")

    serve = sub.add_parser("serve", help="run the control-plane API + dashboard")
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8000)

    worker = sub.add_parser("worker", help="run a worker")
    worker.add_argument("--queue", default="default")
    worker.add_argument("--batch", type=int, default=10)
    worker.add_argument("--lease", type=float, default=30.0, help="lease seconds")

    reaper = sub.add_parser("reaper", help="run the visibility-timeout reaper")
    reaper.add_argument("--interval", type=float, default=1.0)

    publisher = sub.add_parser("publisher", help="run the outbox publisher")
    publisher.add_argument("--stream", default="relay.events")

    args = parser.parse_args(argv)

    if args.command == "migrate":
        migrations.upgrade()
    elif args.command == "version":
        print(__version__)
    elif args.command == "serve":
        _serve(args)
    elif args.command == "worker":
        _worker(args)
    elif args.command == "reaper":
        _reaper(args)
    elif args.command == "publisher":
        _publisher(args)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
