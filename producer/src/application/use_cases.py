import time

from src.application.ports import MessagePublisher
from src.domain.generators import EventGenerator


class ProduceEventsUseCase:
    def __init__(
        self,
        publisher: MessagePublisher,
        topic: str,
        interval: float,
        dlq_topic: str = "events-dlq",
    ):
        self.publisher = publisher
        self.topic = topic
        self.interval = interval
        self.dlq_topic = dlq_topic

    def execute(self) -> None:
        while True:
            try:
                event = EventGenerator.create_random_event()
                envelope = {
                    "spec_version": "1.0",
                    "event_id": event.event_id,
                    "event_type": event.event_type,
                    "source": "core.banking.stream",
                    "partition_key": event.partition_key,
                    "timestamp_utc": event.timestamp_utc,
                    "data": event.payload,
                }
                self.publisher.publish(self.topic, event.partition_key, envelope)
            except Exception as err:
                error_envelope = {
                    "source": "financial-producer",
                    "error_reason": str(err),
                    "timestamp_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                }
                self.publisher.publish_dlq(self.dlq_topic, "error-key", error_envelope)
            time.sleep(self.interval)
