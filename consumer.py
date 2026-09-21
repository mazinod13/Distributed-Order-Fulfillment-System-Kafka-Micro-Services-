import json
import sys
from confluent_kafka import Consumer

GROUP = sys.argv[1] if len(sys.argv) > 1 else "order-printer"

consumer = Consumer({
	"bootstrap.servers":"localhost:9092",
	"group.id": GROUP,
	"auto.offset.reset": "earliest",
        "enable.auto.commit": False,
})

def on_assign(consumer,partitions):
    committed = consumer.committed(partitions, timeout=5)
    for tp in committed:
        print(f"Assigned partition {tp.partition}, commited offset: {tp.offset}")

def on_revoke(consumer,partitions):
    print(f"*** REVOKED partitions {[tp.partition for tp in partitions]}")

consumer.subscribe(["order-events"], on_assign=on_assign, on_revoke=on_revoke)

print("waiting for events. Ctrl + C to stop.")

try:
   while True:
       msg = consumer.poll(1.0)
       if msg is None:
           continue
       if msg.error():
           print(f"Error: {msg.error()}")
           continue

       event = json.loads(msg.value())
       print(
             f"[p{msg.partition()} @{msg.offset()}] {msg.key().decode()}:"
             f"order  {event['order_id']} {event['status']} {event.get('amount')}"
       )
       consumer.commit(message=msg, asynchronous=False)
except KeyboardInterrupt:
    print("\nStopping...")
finally:
    consumer.close()
