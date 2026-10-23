# Fault-Tolerant Job Queue (Python + Redis)

A small background job queue built from scratch on Redis to understand how reliable job processing works: what happens to a job when a worker crashes halfway through it, how failed jobs are retried, and how permanently failing jobs are isolated.

This is a learning project, not a replacement for Celery, SQS or BullMQ.

## What it does

- **Producer** pushes jobs into a Redis queue.
- **Workers** (any number, running in parallel) pick up jobs and process them.
- **Crash recovery:** if a worker dies mid-job, the job is detected and re-queued by a separate **reaper** process.
- **Retries with exponential backoff:** a failed job waits 2s, then 4s, before retrying.
- **Dead-letter queue:** after 3 failed attempts a job is moved to a separate queue instead of retrying forever.
- **Monitor:** a terminal dashboard showing queue depths and throughput.
- **Automated failure test:** runs several workers (some deliberately crashing) and checks that no job is lost.

## How it works

```
producer --> [jobs:pending] --BLMOVE--> [jobs:processing] --done--> removed (ack)
                  ^                            |
                  |                      worker crashes / fails
                  |                            v
          reaper promotes  <---  [jobs:delayed] (wait: 2s, 4s...) 
                                               |
                                      attempts >= 3
                                               v
                                        [jobs:dead]
```

1. A worker takes a job with `BLMOVE`, which **atomically** moves it from `jobs:pending` to `jobs:processing`. The job never exists "nowhere", even if the worker dies right after taking it.
2. The worker records a start timestamp for the job in a sorted set (`jobs:processing:times`).
3. On success, the worker removes the job from `jobs:processing` (acknowledgement).
4. The reaper scans for jobs that have been in `jobs:processing` longer than the **visibility timeout**. Those jobs are treated as failed: their attempt count increases and they move to `jobs:delayed` (retry later) or `jobs:dead` (too many attempts).
5. The same failure handling is used when a handler raises an exception.
6. The move out of `jobs:processing` is done with a small **Lua script**, so removing the job and placing it in its new queue happen as one atomic step.
7. The reaper moves jobs from `jobs:delayed` back to `jobs:pending` once their backoff time has passed.

## Project structure

| File | Purpose |
|---|---|
| `config.py` | Redis connection, queue names, timeout and retry settings |
| `producer.py` | Pushes jobs into the queue (`--count N`, `--bad N` for always-failing jobs) |
| `worker.py` | Picks up and processes jobs (`--crash` simulates random crashes) |
| `reaper.py` | Detects timed-out jobs and promotes delayed retries |
| `jobs.py` | Shared failure handling (Lua scripts for atomic moves) |
| `monitor.py` | Live terminal dashboard |
| `test_system.py` | Automated multi-worker crash/failure test |

## Setup

Requires Python 3 and a running Redis server.

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
redis-cli ping        # should print PONG
```

## Run

Use a separate terminal for each command:

```bash
python reaper.py                    # recovers crashed/failed jobs
python worker.py                    # start one or more workers
python producer.py --count 10       # add jobs (add --bad 2 for always-failing jobs)
python monitor.py                   # optional live dashboard
```

## Automated test

```bash
python test_system.py
```

The test clears Redis, queues 33 jobs (30 normal, 3 that always fail), starts 3 workers (2 of them randomly crash) and a reaper, restarts crashed workers, and waits until everything settles. It then checks that every job is accounted for.

Result from my test runs: 30 jobs completed, the 3 always-failing jobs ended up in the dead-letter queue, and **0 jobs were lost**, with between 4 and 7 worker crashes per run. This was run on a single laptop with a local Redis, with a short visibility timeout (4s) to keep the test fast.

## Guarantees and limitations

**Delivery guarantee: at-least-once.** A job can run more than once, but should not be lost.

Known limitations:

- **No idempotency yet.** If a worker finishes a job's side effect (e.g. sends an email) and crashes before acknowledging, the job will run again and the side effect can happen twice. Handlers would need idempotency keys.
- **Crash simulation is limited.** The test crashes a worker right after it takes a job, before the handler runs. It does not simulate a crash in the middle of a handler.
- **Slow worker vs dead worker.** The reaper cannot tell them apart. A job that runs longer than the visibility timeout is re-queued even if its worker is still alive. There are no worker heartbeats.
- **Redis is a single point of failure.** Persistence settings were not configured; if Redis loses data, jobs are lost.
- **Single reaper.** If it is down, timed-out jobs wait until it restarts (delayed, not lost).
- **Not built:** priorities, rate limiting, authentication, an HTTP API, per-job timeouts inside handlers.

## What I learned

- Why moving a job atomically (`BLMOVE`) matters more than a plain pop.
- How visibility timeouts, retries, backoff and dead-letter queues fit together.
- Why a two-step "remove then re-add" can lose jobs if the process dies between steps, and how a Lua script fixes it.
- How to test failure handling by killing workers on purpose.