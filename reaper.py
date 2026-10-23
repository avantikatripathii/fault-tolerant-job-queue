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

CHECK_INTERVAL = 2  # har kitne seconds mein check karna hai


def reap_once(r) -> int:
    """1) Timeout ho chuki jobs ko processing se nikaalta hai (retry ya dead-letter).
    2) Jin jobs ka retry-wait khatam ho gaya, unhe wapas pending mein daalta hai.
    Kitni timed-out jobs handle hui, wo number return karta hai."""
    now = time.time()

    # Safety: agar koi job processing list mein hai par uska timestamp nahi hai
    # (worker uthate hi crash ho gaya), to usse abhi ka time de do.
    for raw in r.lrange(PROCESSING_LIST, 0, -1):
        r.zadd(PROCESSING_TIMES, {raw: now}, nx=True)

    # Jo jobs VISIBILITY_TIMEOUT se zyada der se processing mein hain
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

    # Jin jobs ka wait khatam ho gaya, unhe wapas pending mein bhejo
    promoted = promote_due_jobs(r)
    if promoted:
        print(f"[reaper] {promoted} job(s) ka wait khatam, wapas pending mein")

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