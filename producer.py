import argparse
import json
import time
import uuid

from config import PENDING_QUEUE, get_redis

JOB_TYPES = ["send_email", "process_file", "resize_image"]


def make_job(job_type: str, payload: dict) -> dict:
    return {
        "id": str(uuid.uuid4()),
        "type": job_type,
        "payload": payload,
        "attempts": 0,
        "created_at": time.time(),
    }


def enqueue(r, job: dict) -> None:
    # LPUSH = left se daalo, worker right se uthayega => FIFO order
    r.lpush(PENDING_QUEUE, json.dumps(job))


def main():
    parser = argparse.ArgumentParser(description="Dummy jobs queue mein daalo")
    parser.add_argument("--count", type=int, default=10, help="kitne normal jobs daalne hain")
    parser.add_argument(
        "--bad", type=int, default=0, help="kitni hamesha-fail hone wali jobs daalni hain (testing)"
    )
    args = parser.parse_args()

    r = get_redis()
    for i in range(args.count):
        job_type = JOB_TYPES[i % len(JOB_TYPES)]
        job = make_job(job_type, {"item": i})
        enqueue(r, job)
        print(f"[producer] queued {job['type']} id={job['id'][:8]}")

    for i in range(args.bad):
        job = make_job("always_fail", {"item": i})
        enqueue(r, job)
        print(f"[producer] queued {job['type']} id={job['id'][:8]}  <-- bad job")

    print(f"[producer] done. queue length = {r.llen(PENDING_QUEUE)}")


if __name__ == "__main__":
    main()
