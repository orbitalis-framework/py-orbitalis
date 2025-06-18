from dataclasses import dataclass
from abc import ABC
from datetime import datetime


@dataclass(frozen=True)
class Descriptor(ABC):
    identifier: str
    created_at: datetime


