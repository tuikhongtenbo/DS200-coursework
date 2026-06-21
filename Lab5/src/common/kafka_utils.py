from kafka import KafkaProducer, KafkaConsumer
import json
import selectors

if hasattr(selectors, 'SelectSelector'):
    _original_unregister = selectors.SelectSelector.unregister
    def _safe_unregister(self, fileobj):
        try:
            return _original_unregister(self, fileobj)
        except (ValueError, KeyError):
            pass
    selectors.SelectSelector.unregister = _safe_unregister

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
