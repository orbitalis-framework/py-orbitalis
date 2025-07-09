from datetime import datetime
from dataclasses import dataclass, field
from typing import FrozenSet, Dict


@dataclass(frozen=True, kw_only=True)
class PluginDescriptor:
    identifier: str
    created_at: float = field(default_factory=lambda: datetime.now())
    operations: Dict[str, Dict]   # operation_name => input_schema
