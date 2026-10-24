import json
import time

from config import (
    BACKOFF_BASE,
    DEAD_QUEUE,
    DELAYED_SET,
    MAX_ATTEMPTS,
    PENDING_QUEUE,
    PROCESSING_LIST,
    PROCESSING_TIMES,
)

# These small Lua scripts run inside Redis as a single atomic step.
# No crash or other command can interleave: either every step happens or none does.
# This is what prevents a job from being lost halfway through a move.
#
# KEYS[1] = processing list, KEYS[2] = processing times, KEYS[3] = delayed set, KEYS[4] = dead queue
# ARGV[1] = old job (raw), ARGV[2] = new job (attempts + 1),
# ARGV[3] = "retry" or "dead", ARGV[4] = time at which to retry (timestamp)
_FAIL_SCRIPT = """
local removed = redis.call('LREM', KEYS[1], 1, ARGV[1])
redis.call('ZREM', KEYS[2], ARGV[1])
if removed == 0 then
    return 0
end
if ARGV[3] == 'dead' then
    redis.call('LPUSH', KEYS[4], ARGV[2])
else
    redis.call('ZADD', KEYS[3], ARGV[4], ARGV[2])
end
return 1
"""

# KEYS[1] = delayed set, KEYS[2] = pending queue, ARGV[1] = current time
# Moves jobs whose backoff wait is over from the delayed set to the pending queue.
_PROMOTE_SCRIPT = """
local due = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1])
for _, raw in ipairs(due) do
    redis.call('ZREM', KEYS[1], raw)
    redis.call('RPUSH', KEYS[2], raw)
end
return #due
"""


def fail_job(r, raw: str, reason: str):
    """Handle a failed job (used by both the worker and the reaper).

    - Increment the attempt count
    - attempts < MAX_ATTEMPTS  => move to the delayed set (retry after a wait)
    - attempts >= MAX_ATTEMPTS => move to the dead-letter queue
    - Removing from processing and adding to the new place happen in one atomic step

    The wait doubles each time (exponential backoff):
    2s after the first failure, 4s after the second (with BACKOFF_BASE = 2).

    Returns: "retry", "dead", or None (if another process already handled the job).
    """
    job = json.loads(raw)
    job["attempts"] += 1
    job["last_error"] = reason
    new_raw = json.dumps(job)

    target = "dead" if job["attempts"] >= MAX_ATTEMPTS else "retry"
    delay = BACKOFF_BASE ** job["attempts"]
    ready_at = time.time() + delay

    moved = r.eval(
        _FAIL_SCRIPT,
        4,
        PROCESSING_LIST,
        PROCESSING_TIMES,
        DELAYED_SET,
        DEAD_QUEUE,
        raw,
        new_raw,
        target,
        ready_at,
    )
    return target if moved else None


def promote_due_jobs(r) -> int:
    """Move jobs whose retry wait is over back to the pending queue.
    Returns the number of jobs moved."""
    return r.eval(_PROMOTE_SCRIPT, 2, DELAYED_SET, PENDING_QUEUE, time.time())