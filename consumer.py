import json
from confluent_kafka import Consumer

consumer = Consumer({
	"bootstrap.servers":"localhost:9092",
	"group.id": "order-printer",
	"auto.offset.reset": "earliest",
        "enable.auto.commit": False,
})

def on_assign(consumer,partitions):
    committed = consumer.committed(partitions, timeout=5)
    for tp in committed:
        print(f"Assigned partition {tp.partition}, commited offset: {tp.offset}")

consumer.subscribe(["order-events"], on_assign=on_assign)

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
