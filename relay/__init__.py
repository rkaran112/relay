"""relay: a Postgres-backed distributed task queue.

At-least-once delivery with exactly-once side effects, built on a transactional
outbox, caller-supplied idempotency keys, and ``FOR UPDATE SKIP LOCKED`` leasing.
See SPEC.md for the full design and build phases.
"""

__version__ = "0.1.0"
