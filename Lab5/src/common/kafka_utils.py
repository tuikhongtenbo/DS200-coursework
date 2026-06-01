"""
Các hàm tiện ích khởi tạo Kafka Producer và Consumer.
"""

from kafka import KafkaProducer, KafkaConsumer
import json

def get_kafka_producer(broker):
    return KafkaProducer(
        bootstrap_servers=[broker],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

def get_kafka_consumer(topic, broker, group_id='default_group'):
    return KafkaConsumer(
        topic,
        bootstrap_servers=[broker],
        group_id=group_id,
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )
