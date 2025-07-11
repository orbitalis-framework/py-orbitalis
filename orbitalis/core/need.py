from dataclasses import dataclass, field
from typing import Optional, Set, Dict, List, Any

from dataclasses_avroschema import AvroModel

from orbitalis.events.discover import NeededOperationInformation
from orbitalis.utils.allowblocklist import AllowBlockListMixin


@dataclass
class Need:
    """
    min: minimum number (mandatory excluded)
    max: maximum number (mandatory excluded)
    mandatory: identifiers
    """

    minimum: int = field(default=0)
    maximum: Optional[int] = field(default=None)
    mandatory: Optional[List[str]] = field(default=None)
    input_schemas: Optional[List[str]] = field(default=None)
    output_schemas: Optional[List[str]] = field(default=None)

    def __post_init__(self):
        if self.minimum < 0 or (self.maximum is not None and self.maximum < 0) \
                or (self.maximum is not None and self.minimum > self.maximum):
            raise ValueError("minimum and/or maximum value are invalid")


@dataclass
class ConstrainedNeed(Need, AllowBlockListMixin):
    """

    Author: Nicola Ricciardi
    """
