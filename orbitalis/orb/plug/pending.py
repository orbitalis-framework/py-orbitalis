from dataclasses import dataclass, field
import datetime
from typing import Set
from abc import ABC


@dataclass(frozen=True, kw_only=True)
class PendingRequest(ABC):
    created_at: float = field(default_factory=lambda: datetime.datetime.now(datetime.timezone.utc).timestamp())
    related_topics: Set[str] = field(default_factory=set)



