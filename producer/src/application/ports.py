from abc import ABC, abstractmethod
from typing import Dict, Any

class MessagePublisher(ABC):
    @abstractmethod
    def publish(self, topic: str, key: str, message: Dict[str, Any]) -> None:
        pass
