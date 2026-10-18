import json
import logging
import sys
import time
from typing import Any, Dict

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from src.application.ports import MessagePublisher

logger = logging.getLogger(__name__)


class KafkaMessagePublisher(MessagePublisher):
    def __init__(self, bootstrap_servers: str):
        self.producer = self._init_producer(bootstrap_servers)

    def _init_producer(self, bootstrap_servers: str) -> KafkaProducer:
        for _ in range(10):
            try:
                p = KafkaProducer(
                    bootstrap_servers=bootstrap_servers.split(","),
                    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode(
                        "utf-8"
                    ),
                    key_serializer=lambda k: str(k).encode("utf-8"),
                    acks=1,
                    retries=3,
                    batch_size=16384,
                    linger_ms=50,
                    compression_type="gzip",
                )
                logger.info("Kafka conectado com sucesso.")
                return p
            except NoBrokersAvailable:
                time.sleep(3)
        sys.exit(1)

    def publish(self, topic: str, key: str, message: Dict[str, Any]) -> None:
        self.producer.send(topic=topic, key=key, value=message)

    def publish_dlq(
        self, dlq_topic: str, key: str, error_payload: Dict[str, Any]
    ) -> None:
        try:
            self.producer.send(topic=dlq_topic, key=key, value=error_payload)
            logger.info(f"Mensagem de erro enviada para DLQ: {dlq_topic}")
        except Exception as e:
            logger.error(f"Falha ao enviar mensagem para DLQ {dlq_topic}: {e}")
