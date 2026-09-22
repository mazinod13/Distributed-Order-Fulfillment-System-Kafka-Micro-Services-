import json
import sys
import random
from collections import defaultdict
from confluent_kafka import Producer


TOPIC = sys.argv[1] if len(sys.argv) > 1 else "order-lifecycle"
USE_KEYS = "--no-keys" not in sys.argv

NUM_ORDERS = 20
TOTAL_EVENTS = 1000

producer = Producer({
    "bootstrap.servers": "localhost:9092",
    "partitioner": "murmur2_random",
    "enable.idempotence": True,
    "sticky.partitioning.linger.ms": 0,
})

next_seq = defaultdict(int)       # order_id -> last seq sent
partition_of = {}                 # order_id -> partition it landed in
violations = 0

def on_delivery(err,msg):
    global violations
    if err is not None:
        print(f"FAILED: {err}")
        return
    key = json.loads(msg.value())["order_id"]
    first = partition_of.setdefault(key, msg.partition())
    if first != msg.partition():
        violations += 1
        print(f"!!! order {key} went to partition {msg.partition()}, earlier to {first}")

for _ in range(TOTAL_EVENTS):
    order_id = str(random.randint(1, NUM_ORDERS))
    next_seq[order_id] += 1
    event = {"order_id": order_id, "seq": next_seq[order_id]}

    producer.produce(
        TOPIC,
        key=order_id.encode("utf-8") if USE_KEYS else None,
        value=json.dumps(event).encode("utf-8"),
        on_delivery=on_delivery,
    )
    producer.poll(0)

producer.flush()

print(f"\nSent {TOTAL_EVENTS} events for {NUM_ORDERS} orders. Partition violations: {violations}")
print("Which orders landed in which partition:")
by_partition = defaultdict(list)
for key, p in partition_of.items():
    by_partition[p].append(int(key))
for p in sorted(by_partition):
    print(f"  partition {p}: orders {sorted(by_partition[p])}")
