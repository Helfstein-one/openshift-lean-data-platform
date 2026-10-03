import logging
import os

from src.application.use_cases import ProduceEventsUseCase
from src.infrastructure.kafka_publisher import KafkaMessagePublisher

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)


def main():
    bootstrap = os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS", "data-cluster-kafka-bootstrap.data-platform.svc:9092"
    )
    topic = os.getenv("KAFKA_TOPIC", "eventos-financeiros")
    interval = float(os.getenv("EMISSION_INTERVAL_SEC", "0.6"))

    publisher = KafkaMessagePublisher(bootstrap)
    use_case = ProduceEventsUseCase(publisher, topic, interval)
    use_case.execute()


if __name__ == "__main__":
    main()
