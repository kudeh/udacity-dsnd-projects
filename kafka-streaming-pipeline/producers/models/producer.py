"""Producer base-class providing common utilites and functionality"""
import os
import logging
import time


from confluent_kafka import avro
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer

logger = logging.getLogger(__name__)

KAFKA_BROKER_HOST_URL = os.getenv("KAFKA_BROKER_HOST_URL", "PLAINTEXT://localhost:9092,PLAINTEXT://localhost:9093,PLAINTEXT://localhost:9094")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")


class Producer:
    """Defines and provides common functionality amongst Producers"""

    # Tracks existing topics across all Producer instances
    existing_topics = set([])

    def __init__(
        self,
        topic_name,
        key_schema,
        value_schema=None,
        num_partitions=1,
        num_replicas=1,
    ):
        """Initializes a Producer object with basic settings"""
        self.topic_name = topic_name
        self.key_schema = key_schema
        self.value_schema = value_schema
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas


        # Configure the broker properties
        self.broker_properties = {
            "bootstrap.servers": KAFKA_BROKER_HOST_URL,
            "schema.registry.url": SCHEMA_REGISTRY_URL,
            "group.id": f"{self.topic_name}",
        }

        # If the topic does not already exist, try to create it
        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)

        # Configure the AvroProducer
        self.producer = AvroProducer(config=self.broker_properties, default_key_schema=self.key_schema, default_value_schema=self.value_schema)

    def create_topic(self):
        """Creates the producer topic if it does not already exist"""
        client = AdminClient({"bootstrap.servers": KAFKA_BROKER_HOST_URL})
        futures = client.create_topics(
            [NewTopic(topic=self.topic_name, num_partitions=self.num_partitions, replication_factor=self.num_replicas)]
        )
        for _, future in futures.items():
            try:
                future.result()
                logger.info("created topic, %s", self.topic_name)
            except Exception as e:
                logger.warning("Failed to create topic, %s", e)
                return

    def time_millis(self):
        return int(round(time.time() * 1000))

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        try:
            # Flush any outstanding messages in the producer buffer
            self.producer.flush()
            logger.info("flused producer to topic: %s", self.topic_name)
        except Exception as e:
            logger.warning("Failed to close producer")

    def time_millis(self):
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))
