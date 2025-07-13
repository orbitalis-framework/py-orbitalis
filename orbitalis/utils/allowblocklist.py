from dataclasses import dataclass,field
from abc import ABC
from typing import Optional, Dict, Set, List, Self


@dataclass(kw_only=True)
class AllowBlockListMixin(ABC):
    """
    allowlist: admitted Orbs (by identifiers)
    blocklist: not admitted Orbs (by identifiers)
    priority: identifier => priority (int)

    Author: Nicola Ricciardi
    """

    allowlist: Optional[List[str]] = field(default=None)
    blocklist: Optional[List[str]] = field(default=None)

    def __post_init__(self):
        if self.allowlist is not None and self.blocklist is not None:
            raise ValueError("allowlist and blocklist can not be used together")

    @classmethod
    def allow_only(cls, identifier: str) -> Self:
        return cls(allowlist=[identifier])

    def is_compliance(self, identifier: str) -> bool:
        if self.blocklist is not None and identifier in self.blocklist:
            return False

        if self.allowlist is not None and identifier not in self.allowlist:
            return False

        return True

