import redis

r = redis.from_url("redis://localhost:6379/0", decode_responses=True)

CLAIM_SCRIPT = """
local current_owner = redis.call('GET', KEYS[1])

if current_owner == false then
    redis.call('SET', KEYS[1], ARGV[1])
    return "CLAIMED"
else
    return current_owner
end
"""

import threading
claim = r.register_script(CLAIM_SCRIPT)
r.delete("toy_claim_key")
results = {}

def try_claim(name):
    results[name] = claim(keys=["toy_claim_key"], args=[name])

t1 = threading.Thread(target=try_claim, args=("worker-A",))
t2 = threading.Thread(target=try_claim, args=("worker-B",))
t1.start(); t2.start()
t1.join(); t2.join()

print(results)