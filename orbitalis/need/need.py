from dataclasses import dataclass, field
from typing import Dict, Optional, Set


@dataclass
class Need:
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
        if self.minimum < 0 or self.maximum < 0 or self.minimum > self.maximum:
            raise ValueError("minimum and/or maximum value are invalid")

        if self.allowlist is not None and self.blocklist is not None:
            raise ValueError("allowlist and blocklist can not be used together")


@dataclass
class Needs:
    mandatory_plugins_by_identifier: Set[str] = field(default_factory=frozenset)
    optional_plugins_by_identifier: Set[str] = field(default_factory=frozenset)
    mandatory_plugins_by_tag: Dict[str, Need] = field(default_factory=dict)
    optional_plugins_by_tag: Dict[str, Need] = field(default_factory=dict)

    def __post_init__(self):
        if self.mandatory_plugins_by_identifier.intersection(self.optional_plugins_by_identifier):
            raise ValueError("A plugin identifier can be mandatory OR optional, not both")

        if set(self.mandatory_plugins_by_tag.keys()).intersection(self.optional_plugins_by_tag.keys()):
            raise ValueError("A plugin tag can be mandatory OR optional, not both")

    def something_needed(self) -> bool:
        return len(self.mandatory_plugins_by_identifier) != 0 or \
                len(self.mandatory_plugins_by_tag.keys()) != 0
