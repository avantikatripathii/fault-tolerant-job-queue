"""Automatic end-to-end test.

Ye script khud:
  1. Redis saaf karti hai
  2. Jobs daalti hai (achhi + hamesha-fail hone wali)
  3. Kai workers + reaper start karti hai (kuch workers jaan-boojh kar crash karte hain)
  4. Crash hue workers ko wapas start karti rehti hai
  5. Jab queue khali ho jaye to check karti hai: koi job kho to nahi gayi?
"""
import json
import os
import subprocess
import sys
import time
import uuid

from config import (
    DEAD_QUEUE,
    DELAYED_SET,
    PENDING_QUEUE,
    PROCESSING_LIST,
    PROCESSING_TIMES,
    STATS_DONE,
    get_redis,
)

NUM_WORKERS = 3        # kul workers
CRASH_WORKERS = 2      # inme se kitne crash mode mein
CRASH_RATE = 0.15      # crash mode wale worker kitni probability se crash karein
GOOD_JOBS = 30
BAD_JOBS = 3           # hamesha-fail hone wali jobs
TEST_VISIBILITY_TIMEOUT = 4   # test ke liye chhota timeout, taaki jaldi khatam ho
MAX_WAIT = 180         # itne seconds se zyada lage to test fail

HERE = os.path.dirname(os.path.abspath(__file__))
GOOD_TYPES = ["send_email", "process_file", "resize_image"]


def make_job(job_type, i):
    return {
        "id": str(uuid.uuid4()),
        "type": job_type,
        "payload": {"item": i},
        "attempts": 0,
        "created_at": time.time(),
    }


def start(args, env):
    return subprocess.Popen(
        [sys.executable] + args,
        cwd=HERE,
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def main():
    r = get_redis()
    r.delete(
        PENDING_QUEUE, PROCESSING_LIST, PROCESSING_TIMES, DEAD_QUEUE, DELAYED_SET, STATS_DONE
    )

    for i in range(GOOD_JOBS):
        r.lpush(PENDING_QUEUE, json.dumps(make_job(GOOD_TYPES[i % 3], i)))
    for i in range(BAD_JOBS):
        r.lpush(PENDING_QUEUE, json.dumps(make_job("always_fail", i)))
    total = GOOD_JOBS + BAD_JOBS
    print(f"[test] {total} jobs queued ({GOOD_JOBS} good + {BAD_JOBS} bad)")

    env = dict(os.environ, VISIBILITY_TIMEOUT=str(TEST_VISIBILITY_TIMEOUT))

    worker_cmds = []
    for i in range(NUM_WORKERS):
        cmd = ["worker.py"]
        if i < CRASH_WORKERS:
            cmd += ["--crash", "--crash-rate", str(CRASH_RATE)]
        worker_cmds.append(cmd)

    workers = [start(c, env) for c in worker_cmds]
    reaper = start(["reaper.py"], env)
    print(f"[test] {NUM_WORKERS} workers ({CRASH_WORKERS} crash-prone) + reaper started")

    restarts = 0
    quiet_checks = 0
    started_at = time.time()
    timed_out = False

    try:
        while True:
            # Crash hue workers ko wapas start karo
            for i, p in enumerate(workers):
                if p.poll() is not None:
                    restarts += 1
                    workers[i] = start(worker_cmds[i], env)

            pending = r.llen(PENDING_QUEUE)
            processing = r.llen(PROCESSING_LIST)
            delayed = r.zcard(DELAYED_SET)
            done = int(r.get(STATS_DONE) or 0)
            dead = r.llen(DEAD_QUEUE)
            print(
                f"[test] pending={pending} processing={processing} delayed={delayed} "
                f"done={done} dead={dead} worker_restarts={restarts}"
            )

            # Sab khali ho gaya? (2 baar lagataar check, taaki beech ka gap miss na ho)
            if pending == 0 and processing == 0 and delayed == 0:
                quiet_checks += 1
                if quiet_checks >= 2:
                    break
            else:
                quiet_checks = 0

            if time.time() - started_at > MAX_WAIT:
                timed_out = True
                break
            time.sleep(2)
    finally:
        for p in workers + [reaper]:
            p.terminate()

    # ---- Result check ----
    done = int(r.get(STATS_DONE) or 0)
    dead_jobs = [json.loads(x) for x in r.lrange(DEAD_QUEUE, 0, -1)]
    bad_in_dead = sum(1 for j in dead_jobs if j["type"] == "always_fail")
    good_in_dead = len(dead_jobs) - bad_in_dead
    lost = total - done - len(dead_jobs)

    print("\n========== RESULT ==========")
    print(f"total jobs            : {total}")
    print(f"completed (done)      : {done}")
    print(f"dead-letter queue     : {len(dead_jobs)} (bad: {bad_in_dead}, good-but-crashed-3x: {good_in_dead})")
    print(f"worker crashes/restarts: {restarts}")
    print(f"LOST jobs             : {lost}")

    ok = (not timed_out) and lost == 0 and bad_in_dead == BAD_JOBS
    print("TEST PASSED" if ok else "TEST FAILED")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()