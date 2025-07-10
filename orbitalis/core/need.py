from dataclasses import dataclass, field
from typing import Optional, Set, Dict, List, Any

from dataclasses_avroschema import AvroModel

from orbitalis.utils.allowblocklist import AllowBlockPriorityListMixin


@dataclass
class Need:
    """
    min: minimum number (includes mandatory)
    max: maximum number (includes mandatory)
    mandatory: identifiers
    """

    minimum: int = field(default=0)
    maximum: Optional[int] = field(default=None)
    mandatory: Optional[List[str]] = field(default=None)
    schema_fingerprint: Optional[str] = field(default=None)

    def __post_init__(self):
        if self.minimum < 0 or (self.maximum is not None and self.maximum < 0) \
                or (self.maximum is not None and self.minimum > self.maximum) \
                or (self.mandatory is not None and len(self.mandatory) > self.maximum):
            raise ValueError("minimum and/or maximum value are invalid")


@dataclass
class ConstrainedNeed(Need, AllowBlockPriorityListMixin, AvroModel):

    def to_need(self) -> Need:
        return Need(
            maximum=self.maximum,
            minimum=self.minimum,
            mandatory=self.mandatory
        )
