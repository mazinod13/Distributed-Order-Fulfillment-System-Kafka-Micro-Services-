import json
from confluent_kafka import Producer

producer = Producer({"bootstrap.servers": "localhost:9092",
		     "partitioner": "murmur2_random",
})


def on_delivery(err, msg):
	if err is not None:
	   print(f"Delivery failed: {err}")
	else:
	   print(f"Delivered to {msg.topic()} [partition{msg.partition()} at offset{msg.offset()}]")

event = {"order_id": 1, "customer": "alice", "status": "created"}

producer.produce(
	topic="order-events",
	key="alice".encode("utf-8"),
	value=json.dumps(event).encode("utf-8"),
	on_delivery=on_delivery,
)

print("producer() returned")

producer.flush()
print("flush() returned")
