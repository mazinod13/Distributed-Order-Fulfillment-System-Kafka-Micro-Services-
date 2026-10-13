from confluent_kafka.admin import AdminClient, NewTopic

BOOTSTRAP = "localhost:9092"
PARTITIONS = 6
REPLICATION = 1

TOPICS = [
      "order.events",
      "inventory.events",
      "payment.events",
      "shipping.events",
]

admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
existing = admin.list_topics(timeout=5).topics

to_create = [
    NewTopic(name,num_partitions=PARTITIONS,replication_factor=REPLICATION)
    for name in TOPICS
    if name not in existing

]

for name in TOPICS:
     if name in existing:
         print(f"exists {name}")

if to_create:
    futures = admin.create_topics(to_create)
    for name, future in futures.items():
        try:
            future.result()
            print(f"created {name}")
        except Exception as e:
            print(f"FAILED {name}: {e}")
