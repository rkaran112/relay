"""A publisher that XADDs one event at a time with a delay, printing each id. The
crash test kills it mid-batch (after some XADDs, before the transaction commits) to
prove no event is lost.

Run as: python slow_publisher.py <dsn> <redis_url> <stream> <per_event_delay>
"""

from __future__ import annotations

import sys

from relay.publisher import Publisher


def main() -> None:
    dsn, redis_url, stream, delay = sys.argv[1], sys.argv[2], sys.argv[3], float(sys.argv[4])

    def announce(event_id: int) -> None:
        print(f"xadd {event_id}", flush=True)

    Publisher(
        dsn, redis_url, stream=stream, batch=10_000, per_event_delay=delay, on_event=announce
    ).drain()


if __name__ == "__main__":
    main()
