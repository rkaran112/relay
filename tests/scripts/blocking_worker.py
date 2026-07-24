"""A worker that leases one job, commits the lease, prints its id, then blocks
forever inside the "handler". Used by the SIGKILL test: the parent kills this
process mid-handler, leaving a committed lease for the reaper to reclaim.

Run as: python blocking_worker.py <dsn> <queue>
"""

from __future__ import annotations

import sys
import time
from datetime import timedelta
from uuid import uuid4

from relay.db import connect
from relay.queue import dequeue


def main() -> None:
    dsn, queue = sys.argv[1], sys.argv[2]
    owner = uuid4()
    with connect(dsn) as conn:
        while True:
            jobs = dequeue(conn, queue, owner=owner, lease=timedelta(seconds=2), batch=1)
            if jobs:
                # Lease is committed. Announce it, then simulate a handler that never
                # returns (a hung / suspended worker) so the parent can kill us here.
                print(f"leased {jobs[0].id}", flush=True)
                time.sleep(3600)
            time.sleep(0.02)


if __name__ == "__main__":
    main()
