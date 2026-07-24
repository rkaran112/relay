"""The invariants the chaos harness asserts after each run heals and drains.

These are the actual correctness contract: no duplicate side effects, no lost jobs,
no stuck leases, dead letters only when truly exhausted, and every event eventually
published. A violation is returned as a human-readable string; an empty list means
the run held.
"""

from __future__ import annotations

from psycopg import Connection
from psycopg.rows import TupleRow


def check_invariants(conn: Connection[TupleRow], enqueued_ids: set[int]) -> list[str]:
    violations: list[str] = []

    # 1. Exactly one side effect per enqueued job — no duplicates, no losses.
    effect_ids = [
        row[0] for row in conn.execute("SELECT job_id FROM side_effects").fetchall()
    ]
    distinct_effects = set(effect_ids)
    if len(effect_ids) != len(distinct_effects):
        dupes = len(effect_ids) - len(distinct_effects)
        violations.append(f"invariant 1: {dupes} duplicate side effect(s)")
    if distinct_effects != enqueued_ids:
        missing = len(enqueued_ids - distinct_effects)
        extra = len(distinct_effects - enqueued_ids)
        if missing:
            violations.append(f"invariant 1: {missing} enqueued job(s) never ran (lost)")
        if extra:
            violations.append(f"invariant 1: {extra} side effect(s) for unknown jobs")

    # 2. Every side effect maps to a job in state 'succeeded'.
    bad = conn.execute(
        "SELECT count(*) FROM side_effects s "
        "JOIN jobs j ON j.id = s.job_id WHERE j.state <> 'succeeded'"
    ).fetchone()
    if bad is not None and bad[0]:
        violations.append(f"invariant 2: {bad[0]} side effect(s) whose job is not 'succeeded'")

    # 3. No job stuck in 'leased' with an expired lease.
    stuck = conn.execute(
        "SELECT count(*) FROM jobs WHERE state = 'leased' AND lease_expires_at < now()"
    ).fetchone()
    if stuck is not None and stuck[0]:
        violations.append(f"invariant 3: {stuck[0]} job(s) stuck in an expired lease")

    # 4. dead_letters only holds jobs that truly exhausted their attempts.
    premature = conn.execute(
        "SELECT count(*) FROM dead_letters d JOIN jobs j ON j.id = d.job_id "
        "WHERE j.attempts < j.max_attempts"
    ).fetchone()
    if premature is not None and premature[0]:
        violations.append(f"invariant 4: {premature[0]} dead letter(s) below max_attempts")

    # 5. Every outbox row for a succeeded job is eventually published.
    unpublished = conn.execute(
        "SELECT count(*) FROM outbox WHERE published_at IS NULL"
    ).fetchone()
    if unpublished is not None and unpublished[0]:
        violations.append(f"invariant 5: {unpublished[0]} outbox event(s) never published")

    return violations
