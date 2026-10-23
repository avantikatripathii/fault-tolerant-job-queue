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

# Ye chhote Lua scripts Redis ke andar ek saath (atomically) chalte hain.
# Matlab beech mein koi crash ya doosra command nahi aa sakta:
# ya to saare steps hote hain, ya koi nahi. Isse job "beech mein kho" nahi sakti.
#
# KEYS[1] = processing list, KEYS[2] = processing times, KEYS[3] = delayed set, KEYS[4] = dead queue
# ARGV[1] = purani job (raw), ARGV[2] = nayi job (attempts +1 ke saath),
# ARGV[3] = "retry" ya "dead", ARGV[4] = retry kab karni hai (timestamp)
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

# KEYS[1] = delayed set, KEYS[2] = pending queue, ARGV[1] = abhi ka time
# Jin jobs ka wait khatam ho gaya, unhe delayed se nikaal kar pending mein daalta hai.
_PROMOTE_SCRIPT = """
local due = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1])
for _, raw in ipairs(due) do
    redis.call('ZREM', KEYS[1], raw)
    redis.call('RPUSH', KEYS[2], raw)
end
return #due
"""


def fail_job(r, raw: str, reason: str):
    """Fail hui job ko handle karta hai (worker aur reaper dono yahi use karte hain).

    - attempts +1 karo
    - attempts < MAX_ATTEMPTS  => delayed set mein (kuch der ruk kar retry)
    - attempts >= MAX_ATTEMPTS => dead-letter queue
    - processing se hatana aur nayi jagah daalna, dono ek hi atomic step mein

    Wait ka time double hota jaata hai (exponential backoff):
    pehli fail ke baad 2 sec, doosri ke baad 4 sec (BACKOFF_BASE = 2 par).

    Return: "retry", "dead", ya None (agar job pehle hi kisi aur ne handle kar li).
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
    """Jin jobs ka wait khatam ho gaya, unhe pending queue mein wapas daalta hai.
    Kitni jobs wapas gayi, wo number return karta hai."""
    return r.eval(_PROMOTE_SCRIPT, 2, DELAYED_SET, PENDING_QUEUE, time.time())