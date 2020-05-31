import pickle
from typing import Any, Dict

from kafka import KafkaConsumer  # type: ignore

from aiven.config import PublishConfig


def run_publish_from_config(user_config: Dict[str, Any]) -> None:
    valid_config = PublishConfig.from_dict(user_config)

    kafka_config = valid_config.broker
    kafka_producer = KafkaConsumer(
        kafka_config.topic, bootstrap_servers=kafka_config.bootstrap_servers
    )

    for message in kafka_producer:
        measurement = pickle.loads(message.value)
        print(measurement)
