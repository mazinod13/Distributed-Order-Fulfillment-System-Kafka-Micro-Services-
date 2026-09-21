import json
import random
import time
from datetime import datetime, timezone

from confluent_kafka import Producer

TOPIC = "order-events"
CUSTOMERS = ["alice", "bob", "carol" , "dave"]
STATUSES = ["created", "paid", "shipped", "cancelled"]

producer = Producer({
	"bootstrap.servers": "localhost:9092",
	"partitioner": "murmur2_random",
})

def on_delivery(err, msg):
	if err is not None:
		print(f"FAILED:{err}")
	else:
		print(f"  -> partition {msg.partition()} offset {msg.offset()}")


def make_event():
	return {
		"order_id": random.randint(1000,1009),
		"customer": random.choice(CUSTOMERS),
		"status": random.choice(STATUSES),
		"amount": round(random.uniform(5,200),2),
		"created_at": datetime.now(timezone.utc).isoformat(),
}
print("producing events.Ctrl + C to stop")
sent = 0
try:
   while True:
        event = make_event()


        producer.produce(
	    topic=TOPIC,
	    key=event["customer"].encode("utf-8"),
	    value=json.dumps(event).encode("utf-8"),
	    on_delivery=on_delivery,
	)
        sent += 1
        print(f"queued #{sent}: {event['customer']} {event['status']} {event['amount']}")

        producer.poll(0)
        time.sleep(1)
except KeyboardInterrupt:
     print("\nStopping...")
finally:
     remaining = producer.flush(10)
     print(f"Done. {sent} queued, {remaining} still undelivered.")
