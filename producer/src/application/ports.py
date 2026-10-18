from abc import ABC, abstractmethod
from typing import Any, Dict


class MessagePublisher(ABC):
    @abstractmethod
    def publish(self, topic: str, key: str, message: Dict[str, Any]) -> None:
        pass

    @abstractmethod
    def publish_dlq(
        self, dlq_topic: str, key: str, error_payload: Dict[str, Any]
    ) -> None:
        pass
