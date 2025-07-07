from dataclasses import dataclass, field
from typing import Dict, Optional, Set


@dataclass
class Need:
    """
    min: minimum number of plugins (includes mandatory)
    max: maximum number of plugins (includes mandatory)
    mandatory: plugins identifiers
    """

    minimum: int = field(default=0)
    maximum: Optional[int] = field(default=None)
    mandatory: Optional[Set[str]] = field(default=None)

    def __post_init__(self):
        if self.minimum < 0 or (self.maximum is not None and self.maximum < 0) \
                or (self.maximum is not None and self.minimum > self.maximum) \
                or (len(self.mandatory) > self.maximum):
            raise ValueError("minimum and/or maximum value are invalid")


@dataclass
class ConstrainedNeed(Need):
    """
    allowlist: admitted Orbs (by plugin identifiers)
    blocklist: not admitted Orbs (by plugin identifiers)
    priority: plugin identifier => priority (int)

    Author: Nicola Ricciardi
    """

    allowlist: Optional[Set[str]] = field(default=None)
    blocklist: Optional[Set[str]] = field(default=None)
    priority: Dict[str, int] = field(default_factory=dict)

    def __post_init__(self):

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

    def to_need(self) -> Need:
        return Need(
            maximum=self.maximum,
            minimum=self.minimum,
            mandatory=self.mandatory
        )



@dataclass
class CoreConfiguration:
    """
    TODO

    Author: Nicola Ricciardi
    """

    discover_topic: str
    discovering_interval: int = field(default=2)
    needs: Dict[str, ConstrainedNeed] = field(default_factory=dict)        # remote_operation => Need
