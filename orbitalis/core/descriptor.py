from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict


@dataclass(frozen=True, kw_only=True)
class CoreDescriptor:
    """

    Author: Nicola Ricciardi
    """

    identifier: str
    created_at: float = field(default_factory=lambda: datetime.now())