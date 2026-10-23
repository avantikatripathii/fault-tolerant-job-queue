import os

import redis

REDIS_HOST = "localhost"
REDIS_PORT = 6379
REDIS_DB = 0

# Redis keys
PENDING_QUEUE = "jobs:pending"            # jobs jo abhi uthaye nahi gaye
PROCESSING_LIST = "jobs:processing"       # jobs jo kisi worker ne utha li hain
PROCESSING_TIMES = "jobs:processing:times"  # har processing job kab uthayi gayi (timestamp)
DEAD_QUEUE = "jobs:dead"                  # dead-letter queue: jo jobs baar-baar fail hui
STATS_DONE = "jobs:stats:done"            # kitni jobs successfully complete hui (counter)
DELAYED_SET = "jobs:delayed"              # retry ka wait kar rahi jobs (score = kab retry karni hai)

# Itne seconds mein job complete na ho to maan lo worker crash ho gaya.
# Environment variable se badal sakte hain (test script isse chhota karta hai).
VISIBILITY_TIMEOUT = int(os.getenv("VISIBILITY_TIMEOUT", "10"))

# Ek job maximum kitni baar try hogi, uske baad dead-letter queue mein
MAX_ATTEMPTS = 3

# Retry se pehle wait: 2 sec, phir 4 sec, phir 8 sec... (har baar double)
BACKOFF_BASE = int(os.getenv("BACKOFF_BASE", "2"))


def get_redis():
    """Redis connection return karta hai. decode_responses=True => bytes ki jagah str milega."""
    return redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        db=REDIS_DB,
        decode_responses=True,
        socket_timeout=30,
    )