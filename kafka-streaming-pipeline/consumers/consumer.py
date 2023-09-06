"""Defines core consumer functionality"""
import logging
import os

import confluent_kafka
from confluent_kafka import Consumer, OFFSET_BEGINNING
from confluent_kafka.avro import AvroConsumer
from confluent_kafka.avro.serializer import SerializerError
from tornado import gen


logger = logging.getLogger(__name__)

KAFKA_BROKER_HOST_URL = "PLAINTEXT://localhost:9092,PLAINTEXT://localhost:9093,PLAINTEXT://localhost:9094"
SCHEMA_REGISTRY_URL = "http://localhost:8081"


class KafkaConsumer:
    """Defines the base kafka consumer class"""

    def __init__(
        self,
        topic_name_pattern,
        message_handler,
        is_avro=True,
        offset_earliest=False,
        sleep_secs=1.0,
        consume_timeout=0.1,
    ):
        """Creates a consumer object for asynchronous use"""
        self.topic_name_pattern = topic_name_pattern
        self.message_handler = message_handler
        self.sleep_secs = sleep_secs
        self.consume_timeout = consume_timeout
        self.offset_earliest = offset_earliest

        # configure broker properties
        self.broker_properties = {
            "bootstrap.servers": KAFKA_BROKER_HOST_URL,
            "group.id": f"{topic_name_pattern}".replace("^",""),
            "auto.offset.reset": "earliest" if offset_earliest else "latest",
        }

        # Create the based on type
        if is_avro is True:
            self.broker_properties["schema.registry.url"] = SCHEMA_REGISTRY_URL
            self.consumer = AvroConsumer(self.broker_properties)
        else:
            self.consumer = Consumer(self.broker_properties)

        # subscribe to topics
        self.consumer.subscribe([self.topic_name_pattern], on_assign=self.on_assign)

    def on_assign(self, consumer, partitions):
        """Callback for when topic assignment takes place"""
        # If the topic is configured to use `offset_earliest` set the partition offset to
        # the beginning or earliest
        if self.offset_earliest:
            for partition in partitions:
                partition.offset = OFFSET_BEGINNING

        logger.info("partitions assigned for %s", self.topic_name_pattern)
        consumer.assign(partitions)

    async def consume(self):
        """Asynchronously consumes data from kafka topic"""
        while True:
            num_results = 1
            while num_results > 0:
                num_results = self._consume()
            await gen.sleep(self.sleep_secs)

    def _consume(self):
        """Polls for a message. Returns 1 if a message was received, 0 otherwise"""
        while True:
            try:
                message = self.consumer.poll(self.consume_timeout)
            except Exception as e:
                logger.error("Error %s, occured polling message from %s", e, self.topic_name_pattern)

            if message is None:
                logger.warning("no message received by consumer")
                return 0
            elif message.error() is not None:
                logger.error("error from consumer, %s", message.error())
                return 0
            else:
                logger.info("consumed message from topic, %s", self.topic_name_pattern)
                self.message_handler(message)
                return 1
            

    def close(self):
        """Cleans up any open kafka consumers"""
        self.consumer.close()
