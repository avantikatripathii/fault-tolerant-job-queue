import argparse
import json
import os
import random
import time

from config import (
    MAX_ATTEMPTS,
    PENDING_QUEUE,
    PROCESSING_LIST,
    PROCESSING_TIMES,
    STATS_DONE,
    get_redis,
)
from jobs import fail_job

WORKER_ID = f"worker-{os.getpid()}"


# ---- Dummy job handlers (demo ke liye sleep se kaam simulate) ----
def send_email(payload):
    time.sleep(random.uniform(0.5, 1.5))


def process_file(payload):
    time.sleep(random.uniform(1, 2))


def resize_image(payload):
    time.sleep(random.uniform(0.3, 1))


def always_fail(payload):
    """Testing ke liye: ye job hamesha fail hoti hai."""
    time.sleep(0.3)
    raise RuntimeError("simulated failure")


HANDLERS = {
    "send_email": send_email,
    "process_file": process_file,
    "resize_image": resize_image,
    "always_fail": always_fail,
}


def handle_job(job: dict) -> None:
    handler = HANDLERS.get(job["type"])
    if handler is None:
        raise ValueError(f"unknown job type: {job['type']}")
    handler(job["payload"])


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--crash",
        action="store_true",
        help="testing ke liye: 30%% jobs par worker jaan-boojh kar crash kar do",
    )
    parser.add_argument(
        "--crash-rate",
        type=float,
        default=0.3,
        help="--crash ke saath: kitni probability se crash kare (0 se 1), default 0.3",
    )
    args = parser.parse_args()

    r = get_redis()
    print(f"[{WORKER_ID}] started, waiting for jobs... (Ctrl+C to stop)")
    if args.crash:
        print(f"[{WORKER_ID}] CRASH MODE ON")

    try:
        while True:
            # BLMOVE = pending se job nikalo aur ATOMICALLY processing list mein daal do.
            raw = r.blmove(PENDING_QUEUE, PROCESSING_LIST, 5, src="RIGHT", dest="LEFT")
            if raw is None:
                continue  # timeout, dobara try

            # Note karo ki job kab uthayi (reaper isi se timeout check karega)
            r.zadd(PROCESSING_TIMES, {raw: time.time()})

            job = json.loads(raw)
            print(
                f"[{WORKER_ID}] picked {job['type']} id={job['id'][:8]} "
                f"(attempt {job['attempts'] + 1}/{MAX_ATTEMPTS})"
            )

            # Crash simulation: bina cleanup ke process turant band
            if args.crash and random.random() < args.crash_rate:
                print(f"[{WORKER_ID}] CRASHING on id={job['id'][:8]} !!!")
                os._exit(1)

            try:
                handle_job(job)
            except Exception as e:
                # Fail hui job: retry ya dead-letter queue
                result = fail_job(r, raw, str(e))
                if result == "dead":
                    print(f"[{WORKER_ID}] DEAD   {job['type']} id={job['id'][:8]}: {e}")
                else:
                    print(f"[{WORKER_ID}] FAILED {job['type']} id={job['id'][:8]}: {e} -> retry")
                continue

            # Job complete: processing se hata do (ack)
            r.lrem(PROCESSING_LIST, 1, raw)
            r.zrem(PROCESSING_TIMES, raw)
            r.incr(STATS_DONE)
            print(f"[{WORKER_ID}] done   {job['type']} id={job['id'][:8]}")
    except KeyboardInterrupt:
        print(f"\n[{WORKER_ID}] stopped.")


if __name__ == "__main__":
    main()
