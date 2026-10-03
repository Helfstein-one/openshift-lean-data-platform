from dataclasses import dataclass
from typing import Any, Dict


@dataclass
class FinancialEvent:
    event_id: str
    event_type: str
    partition_key: str
    timestamp_utc: str
    payload: Dict[str, Any]
