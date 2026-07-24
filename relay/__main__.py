"""relay command-line entry point.

Phase 0 ships two commands: ``migrate`` (apply the schema) and ``version``. The
worker, publisher and API commands are added in later phases.
"""

from __future__ import annotations

import argparse

from . import __version__, migrations


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="relay", description="relay task queue")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("migrate", help="apply database migrations")
    sub.add_parser("version", help="print the relay version")

    args = parser.parse_args(argv)

    if args.command == "migrate":
        migrations.upgrade()
    elif args.command == "version":
        print(__version__)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
