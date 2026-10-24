import os

import redis

REDIS_HOST = "localhost"
REDIS_PORT = 6379
REDIS_DB = 0

# Redis keys
PENDING_QUEUE = "jobs:pending"              # jobs waiting to be picked up
PROCESSING_LIST = "jobs:processing"         # jobs currently held by a worker
PROCESSING_TIMES = "jobs:processing:times"  # when each processing job was picked up (timestamp)
DEAD_QUEUE = "jobs:dead"                    # dead-letter queue: jobs that failed too many times
STATS_DONE = "jobs:stats:done"              # counter of successfully completed jobs
DELAYED_SET = "jobs:delayed"                # jobs waiting to be retried (score = time to retry)

# If a job is not finished within this many seconds, assume the worker crashed.
# Can be overridden with an environment variable (the test script shortens it).
VISIBILITY_TIMEOUT = int(os.getenv("VISIBILITY_TIMEOUT", "10"))

# Maximum number of attempts per job before it goes to the dead-letter queue
MAX_ATTEMPTS = 3

# Wait before a retry doubles each time: 2s, then 4s, then 8s...
BACKOFF_BASE = int(os.getenv("BACKOFF_BASE", "2"))


def get_redis():
    """Return a Redis connection. decode_responses=True returns str instead of bytes."""
    return redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        db=REDIS_DB,
        decode_responses=True,
        socket_timeout=30,
    )