# LinkedIn showcase kit — relay

Links (live):
- Live demo: https://relay-7b2o.onrender.com
- Repo: https://github.com/rkaran112/relay  (private for now — flip to public before
  posting so people can open it)

Note: the Render free web service sleeps after ~15 min idle, so the first visit to the
demo takes ~30–60s to cold-start. Worth saying in the post, or open it a minute before
you publish.

Attach the dashboard screenshot (`docs/dashboard.png`) as the post image — it stops the
scroll and shows something real. Best posting time is Tue–Thu morning.

---

## Post A — the main one (story + proof)

> Most engineers can tell you "use a queue." Fewer can say what happens when a worker
> dies in the exact instant between doing the work and reporting it done.
>
> I built **relay** to understand that failure from the inside: a distributed task queue
> on Postgres where handlers can crash, be killed, or freeze indefinitely — and the
> observable side effect still happens **exactly once**.
>
> The one idea that matters: exactly-once *delivery* is impossible. Exactly-once *side
> effects* are not. You accept at-least-once delivery and make the effect idempotent, so
> a duplicate delivery becomes a no-op.
>
> Three mechanisms get you there, and relay implements all three:
> • effect + "job done" in one Postgres transaction (no gap to crash in)
> • caller-supplied idempotency keys (dedupe at the door)
> • **fencing tokens** — a worker paused past its lease is a zombie; when it wakes and
>   reports success, its stale token is rejected and its whole transaction rolls back
>
> Most candidates know the first. Some know the second. Almost nobody implements the third.
>
> I didn't want to *claim* this, I wanted to *prove* it. So relay ships a chaos harness
> that kills workers mid-handler, forces duplicate deliveries, and reclaims leases under
> live workers, then checks five invariants. **100/100 randomized runs: zero duplicate
> side effects, zero lost jobs.** And a negative control — rip out the fencing check and
> the harness instantly catches the duplicates it's meant to. A suite that never fails
> proves nothing.
>
> Also inside: `SELECT … FOR UPDATE SKIP LOCKED` leasing (8 workers, 10k jobs, each
> handled exactly once), a transactional outbox to Redis Streams, retries with
> full-jitter backoff, a dead-letter queue with replay, OpenTelemetry traces that survive
> the queue hop, and a live dashboard.
>
> No Celery, no SQS. Postgres and Redis only — the queue is the project.
>
> Honest benchmark on a 2-core laptop: ~640 jobs/sec, scaling knee at 4 workers (exactly
> the core count). I wrote down what I measured, not a number that sounds better.
>
> Live demo → https://relay-7b2o.onrender.com
> Code + design write-up → https://github.com/rkaran112/relay
>
> #distributedsystems #backend #postgresql #python #softwareengineering

---

## Post B — short version

> "Exactly-once delivery is impossible. Exactly-once side effects are achievable."
>
> I built a distributed task queue on Postgres to prove I can say that fluently and back
> it with code: transactional outbox, idempotency keys, `FOR UPDATE SKIP LOCKED` leasing,
> and fencing tokens that reject a zombie worker's stale completion.
>
> The part I'm proud of: a chaos harness that kills workers, partitions the DB, and
> replays duplicates — 100/100 runs with zero duplicate effects and zero lost jobs — plus
> a negative control that fails on purpose when I remove the fencing check.
>
> No Celery, no SQS. Live demo + code below.
>
> https://relay-7b2o.onrender.com · https://github.com/rkaran112/relay
> #distributedsystems #backend #postgresql

---

## First comment (drop right after posting — rewards the curious)

> The fencing token is the detail I'd want to be asked about in an interview. A worker
> leases a job with a monotonic token. If its VM is suspended for two minutes, the reaper
> expires the lease and hands the job to someone else with a new token. When the first
> worker wakes and commits, its `UPDATE … WHERE lease_token = $stale` matches zero rows,
> so the whole transaction — side effect included — rolls back. The proof that this
> matters: I remove that one predicate and the chaos harness immediately starts finding
> duplicate side effects. Full write-up and the invariant suite are in the repo.

---

## Carousel outline (optional, high-engagement)

Make 5 simple slides (Canva/Figma, dark background to match the dashboard):

1. **Title** — "How do you charge a card exactly once when workers crash?" / relay
2. **The trap** — a worker can die between doing the work and saying so. Re-run → double
   charge. Drop → never charged. You can't tell which happened.
3. **The reframe** — exactly-once *delivery* is impossible; exactly-once *effects* aren't.
4. **The three mechanisms** — one transaction · idempotency keys · fencing tokens (one
   line each).
5. **The proof** — chaos harness, 100/100 runs, negative control that fails on purpose +
   the dashboard screenshot. CTA: live demo + repo.

---

## Assets checklist

- [ ] `docs/dashboard.png` (already in the repo) as the lead image
- [ ] Optional 10–15s screen recording: enqueue a burst → watch the tiles + sparkline
      move → hit "50 flaky" → replay a dead letter. GIFs/video lift reach.
- [ ] Pin the post to your profile Featured section
- [ ] Make sure `https://github.com/rkaran112/relay` README opens with the exactly-once line and the screenshot

## Talking points if someone comments (interview reps)

- Why `SKIP LOCKED` over `SELECT … FOR UPDATE`? The latter serializes every worker behind
  the first lock holder; SKIP LOCKED hands out disjoint batches with no coordination.
- The dual-write problem and how the outbox solves it (publish intent commits with the DB
  change; a separate publisher relays it at-least-once; consumers dedupe on event id).
- Where the guarantee actually breaks: the external-effect gap between the API call and
  the result write — reconciled by a sweeper. Knowing the edge of your guarantee beats
  pretending it has none.
- The scaling knee: my throughput stops improving past 4 workers because that's the box's
  core count. Naming where a system stops scaling is a senior signal.
