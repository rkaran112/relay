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
import os
from datetime import timedelta

from . import __version__, migrations

# PaaS platforms (Render, Fly, Heroku) assign the port via $PORT.
_DEFAULT_PORT = int(os.environ.get("PORT", "8000"))


def _serve(args: argparse.Namespace) -> None:
    import uvicorn

    from .api import create_app
    from .tracing import configure_tracing

    configure_tracing("relay-api")
    uvicorn.run(create_app(), host=args.host, port=args.port)


def _worker(args: argparse.Namespace) -> None:
    from .config import get_settings
    from .handlers import side_effect_handler
    from .tracing import configure_tracing
    from .worker import Worker

    configure_tracing("relay-worker")
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


def _demo(args: argparse.Namespace) -> None:
    """One process: the API + a pool of workers + the reaper + the publisher, as
    daemon threads. Point a browser at the printed URL and drive it from the UI.
    """
    import threading

    import uvicorn

    from .api import create_app
    from .config import get_settings
    from .handlers import side_effect_handler
    from .publisher import Publisher
    from .reaper import Reaper
    from .tracing import configure_tracing
    from .worker import Worker

    configure_tracing("relay-demo")
    settings = get_settings()
    dsn = settings.psycopg_dsn

    runners = [
        Worker(dsn, args.queue, side_effect_handler, batch=args.batch).run_forever
        for _ in range(args.workers)
    ]
    runners.append(Reaper(dsn).run_forever)

    def publish_quietly() -> None:
        try:
            Publisher(dsn, settings.redis_url).run_forever()
        except Exception as exc:  # Redis may be absent in a demo; don't take the app down
            print(f"relay demo: publisher stopped ({exc})", flush=True)

    runners.append(publish_quietly)
    for run in runners:
        threading.Thread(target=run, daemon=True).start()

    print(
        f"relay demo: {args.workers} workers + reaper + publisher on '{args.queue}' | "
        f"http://{args.host}:{args.port}",
        flush=True,
    )
    uvicorn.run(create_app(dsn), host=args.host, port=args.port, log_level="warning")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="relay", description="relay task queue")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("migrate", help="apply database migrations")
    sub.add_parser("version", help="print the relay version")

    serve = sub.add_parser("serve", help="run the control-plane API + dashboard")
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=_DEFAULT_PORT)

    worker = sub.add_parser("worker", help="run a worker")
    worker.add_argument("--queue", default="default")
    worker.add_argument("--batch", type=int, default=10)
    worker.add_argument("--lease", type=float, default=30.0, help="lease seconds")

    reaper = sub.add_parser("reaper", help="run the visibility-timeout reaper")
    reaper.add_argument("--interval", type=float, default=1.0)

    publisher = sub.add_parser("publisher", help="run the outbox publisher")
    publisher.add_argument("--stream", default="relay.events")

    demo = sub.add_parser("demo", help="run API + workers + reaper + publisher together")
    demo.add_argument("--host", default="127.0.0.1")
    demo.add_argument("--port", type=int, default=_DEFAULT_PORT)
    demo.add_argument("--queue", default="default")
    demo.add_argument("--workers", type=int, default=4)
    demo.add_argument("--batch", type=int, default=10)

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
    elif args.command == "demo":
        _demo(args)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
