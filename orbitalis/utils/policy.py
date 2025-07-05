from dataclasses import dataclass, field
from typing import Dict, Optional, Set


@dataclass
class Policy:
    """
    min: minimum number of Orbs
    max: maximum number of Orbs
    allowlist: admitted Orbs (by identifiers)
    blocklist: not admitted Orbs (by identifiers)

    Author: Nicola Ricciardi
    """

    minimum: int = field(default=0)
    maximum: Optional[int] = field(default=None)
    allowlist: Optional[Set[str]] = field(default=None)
    blocklist: Optional[Set[str]] = field(default=None)
    priority: Dict[str, int] = field(default_factory=dict)

    def __post_init__(self):
        if self.minimum < 0 or (self.maximum is not None and self.maximum < 0) or (self.maximum is not None and self.minimum > self.maximum):
            raise ValueError("minimum and/or maximum value are invalid")

        if self.allowlist is not None and self.blocklist is not None:
            raise ValueError("allowlist and blocklist can not be used together")

    def slot_available(self) -> bool:
        return self.maximum > 0

    def is_compliance(self, identifier: str) -> bool:
        if self.blocklist is not None and identifier in self.blocklist:
            return False

        if self.allowlist is not None and identifier not in self.allowlist:
            return False

        return True