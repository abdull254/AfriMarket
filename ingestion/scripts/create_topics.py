# KafkaAdminClient = a tool for managing Kafka (create, list, delete topics)
# NewTopic = the description of a topic we want to create
from kafka.admin import KafkaAdminClient, NewTopic
# The error Kafka raises when a topic with that name already exists
from kafka.errors import TopicAlreadyExistsError
import config   # our settings: Kafka address, topic names, number of partitions

# Connect to Kafka using the address from config.py ("kafka:9092")
admin = KafkaAdminClient(bootstrap_servers=config.KAFKA_BOOTSTRAP)

# config.TOPICS has two names: "transactions" and "complaints"
# config.TOPICS has "transactions" and "complaints"; we add "invalid_records"
for name in list(config.TOPICS) + [config.INVALID_TOPIC]:
    try:
        # Create the topic:
        #   num_partitions=3   -> split into 3 partitions (lanes) for parallel reading
        #   replication_factor=1 -> keep 1 copy, because we have only 1 Kafka server
        admin.create_topics([NewTopic(name, num_partitions=config.PARTITIONS, replication_factor=1)])
        print(f"Created topic '{name}' with {config.PARTITIONS} partitions")
    except TopicAlreadyExistsError:
        # If you run the script twice, it doesn't crash, it just tells you
        print(f"Topic '{name}' already exists")

# List all topics, hiding Kafka's internal ones (their names start with "__")
print("Topics in Kafka:", [t for t in admin.list_topics() if not t.startswith("__")])

admin.close()   # close the connection cleanly
