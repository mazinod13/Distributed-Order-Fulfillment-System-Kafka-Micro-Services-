import json
import sys
import uuid
from collections import defaultdict
from confluent_kafka import Consumer

TOPIC = sys.argv[1] if len(sys.argv) > 1 else "order-lifecycle"

consumer = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": f"verifier-{uuid.uuid4()}",   # fresh group -> always read from the start
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
})
consumer.subscribe([TOPIC])

partition_of = {}             # order_id -> first partition seen
last_seq = {}                 # order_id -> last seq seen
per_partition = defaultdict(int)
count = partition_errors = order_errors = restarts = 0
idle = 0

print("Reading... stops after 5 s with no new messages.")
while True:
    msg = consumer.poll(1.0)
    if msg is None:
        if count > 0:
            idle += 1
            if idle >= 5:
                break
        continue
    if msg.error():
        print(f"Error: {msg.error()}")
        continue

    idle = 0
    count += 1
    per_partition[msg.partition()] += 1
    event = json.loads(msg.value())
    key = event["order_id"]
    seq = event["seq"]

    # Rule 1: same key -> same partition
    first = partition_of.setdefault(key, msg.partition())
    if first != msg.partition():
        partition_errors += 1
        print(f"!!! order {key} in partition {msg.partition()}, earlier in {first}")

    # Rule 2: seq goes up by exactly 1 (or restarts at 1)
    prev = last_seq.get(key, 0)
    if seq == prev + 1:
        pass
    elif seq == 1:
        restarts += 1
    else:
        order_errors += 1
        print(f"!!! order {key}: seq {seq} came after {prev} (p{msg.partition()} @{msg.offset()})")
    last_seq[key] = seq

consumer.close()

print(f"\nRead {count} messages for {len(partition_of)} orders")
print(f"Per partition: {dict(sorted(per_partition.items()))}")
print(f"Partition violations: {partition_errors}")
print(f"Ordering violations:  {order_errors}")
print(f"Producer restarts:    {restarts}")
