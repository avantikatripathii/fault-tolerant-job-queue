import json
import time

from config import (
    DEAD_QUEUE,
    DELAYED_SET,
    PENDING_QUEUE,
    PROCESSING_LIST,
    STATS_DONE,
    get_redis,
)

REFRESH_SECONDS = 1


def main():
    r = get_redis()
    prev_done = int(r.get(STATS_DONE) or 0)
    prev_time = time.time()

    try:
        while True:
            time.sleep(REFRESH_SECONDS)
            now = time.time()

            pending = r.llen(PENDING_QUEUE)
            processing = r.llen(PROCESSING_LIST)
            delayed = r.zcard(DELAYED_SET)
            done = int(r.get(STATS_DONE) or 0)
            dead = r.llen(DEAD_QUEUE)

            # Pichle refresh se ab tak kitni jobs complete hui => jobs per second
            rate = (done - prev_done) / (now - prev_time)
            prev_done, prev_time = done, now

            lines = [
                "===== JOB QUEUE MONITOR (Ctrl+C to stop) =====",
                f"pending     : {pending}   (queue mein intezaar)",
                f"processing  : {processing}   (worker kaam kar raha hai)",
                f"delayed     : {delayed}   (retry ke wait mein)",
                f"done        : {done}   (complete ho chuki)",
                f"dead        : {dead}   (dead-letter queue)",
                f"throughput  : {rate:.1f} jobs/sec",
                "",
                "Latest dead jobs:",
            ]

            # LPUSH se daali thi, isliye sabse nayi job index 0 par hoti hai
            recent_dead = r.lrange(DEAD_QUEUE, 0, 2)
            if not recent_dead:
                lines.append("  (koi nahi)")
            for raw in recent_dead:
                job = json.loads(raw)
                lines.append(
                    f"  {job['type']} id={job['id'][:8]} "
                    f"attempts={job['attempts']} error={job.get('last_error')}"
                )

            # \033[H\033[J = screen saaf karke upar se dobara print karo (live dashboard jaisa)
            print("\033[H\033[J" + "\n".join(lines), flush=True)
    except KeyboardInterrupt:
        print("\n[monitor] stopped.")


if __name__ == "__main__":
    main()