import json
import time

from config import (
    MAX_ATTEMPTS,
    PROCESSING_LIST,
    PROCESSING_TIMES,
    VISIBILITY_TIMEOUT,
    get_redis,
)
from jobs import fail_job, promote_due_jobs

CHECK_INTERVAL = 2  # seconds between checks


def reap_once(r) -> int:
    """1) Take timed-out jobs out of processing (retry or dead-letter).
    2) Move jobs whose retry wait is over back to the pending queue.
    Returns the number of timed-out jobs handled."""
    now = time.time()

    # Safety net: a job in the processing list with no timestamp means the worker
    # crashed right after taking it. Give it the current time so it can time out later.
    for raw in r.lrange(PROCESSING_LIST, 0, -1):
        r.zadd(PROCESSING_TIMES, {raw: now}, nx=True)

    # Jobs that have been processing for longer than VISIBILITY_TIMEOUT
    expired = r.zrangebyscore(PROCESSING_TIMES, 0, now - VISIBILITY_TIMEOUT)

    handled = 0
    for raw in expired:
        job = json.loads(raw)
        result = fail_job(r, raw, "timed out (worker crashed?)")
        if result == "retry":
            print(
                f"[reaper] retry later {job['type']} id={job['id'][:8]} "
                f"(attempt {job['attempts'] + 1}/{MAX_ATTEMPTS}, timed out)"
            )
            handled += 1
        elif result == "dead":
            print(f"[reaper] DEAD   {job['type']} id={job['id'][:8]} (too many attempts)")
            handled += 1

    # Jobs whose retry wait is over go back to pending
    promoted = promote_due_jobs(r)
    if promoted:
        print(f"[reaper] {promoted} job(s) finished waiting, moved back to pending")

    return handled


def main():
    r = get_redis()
    print(f"[reaper] started. timeout={VISIBILITY_TIMEOUT}s (Ctrl+C to stop)")
    try:
        while True:
            reap_once(r)
            time.sleep(CHECK_INTERVAL)
    except KeyboardInterrupt:
        print("\n[reaper] stopped.")


if __name__ == "__main__":
    main()