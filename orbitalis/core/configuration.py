from dataclasses import dataclass, field
from typing import List, Dict, Optional, Set


@dataclass
class Needed:
    """
    mandatory: `True` if this record is mandatory for core
    min: minimum number of plugins of this type
    max: maximum number of plugins of this type
    allowlist: admitted plugins (by identifiers)
    blocklist: not admitted plugins (by identifiers)

    Author: Nicola Ricciardi
    """

    minimum: Optional[int] = field(default=None)
    maximum: Optional[int] = field(default=None)
    required: Set[str] = field(default_factory=set)
    allowlist: Optional[Set[str]] = field(default=None)
    blocklist: Optional[Set[str]] = field(default=None)
    priority: Dict[str, int] = field(default_factory=dict)

    def __post_init__(self):
        if self.minimum < 0 or self.maximum < 0 or self.minimum > self.maximum:
            raise ValueError("minimum and/or maximum value are invalid")

        if self.allowlist is not None and self.blocklist is not None:
            raise ValueError("allowlist and blocklist can not be used together")

        if self.blocklist is not None and len(self.required.intersection(self.blocklist)) > 0:
            raise ValueError(f"{self.required.intersection(self.blocklist)} are both in required and blocklist")

        if len(self.required) > self.maximum:
            raise ValueError("required is greater than maximum")


@dataclass
class Feature:
    name: str
    description: Optional[str] = field(default=None)

    mandatory_plugins_by_identifier: Set[str] = field(default_factory=frozenset)
    optional_plugins_by_identifier: Set[str] = field(default_factory=frozenset)
    mandatory_plugins_by_tag: Dict[str, Needed] = field(default_factory=dict)
    optional_plugins_by_tag: Dict[str, Needed] = field(default_factory=dict)



@dataclass
class CoreConfiguration:
    """
    TODO

    Author: Nicola Ricciardi
    """

    features: Set[Feature]

    discovering_interval: int = field(default=2)