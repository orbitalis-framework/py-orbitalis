from dataclasses import dataclass, field
from abc import ABC, abstractmethod
import datetime


@dataclass(frozen=True, kw_only=True)
class Descriptor(ABC):
    identifier: str
    created_at: float = field(default_factory=lambda: datetime.datetime.now(datetime.timezone.utc).timestamp())

