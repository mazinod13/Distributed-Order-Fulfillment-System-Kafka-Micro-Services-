from confluent_kafka.admin import AdminClient

admin = AdminClient({"bootstrap.servers": "localhost:9092"})

metadata = admin.list_topics(timeout=5)
print("Cluster ID:",metadata.cluster_id)
print("Brokers:" ,metadata.brokers)
print("Topics:" ,list(metadata.topics.keys()))
