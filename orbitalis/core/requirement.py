from dataclasses import dataclass, field
from typing import Optional, List

from busline.client.subscriber.event_handler.event_handler import EventHandler
from orbitalis.orbiter.schemaspec import Inputs, Outputs
from orbitalis.utils.allowblocklist import AllowBlockListMixin


@dataclass(kw_only=True)
class Constraint(AllowBlockListMixin, Inputs, Outputs):
    """
    Constraint usable for needs

    min: minimum number (mandatory included)
    max: maximum number (mandatory included)
    mandatory: identifiers

    Author: Nicola Ricciardi
    """

    minimum: int = field(default=0)
    maximum: Optional[int] = field(default=None)
    mandatory: List[str] = field(default_factory=list)


    def __post_init__(self):
        super().__post_init__()

        if self.minimum < 0 or (self.maximum is not None and self.maximum < 0) \
                or (self.maximum is not None and self.minimum > self.maximum):
            raise ValueError("Minimum and/or maximum value are invalid")

        if len(self.inputs) == 0:
            raise ValueError("Missed inputs")

        if len(self.outputs) == 0:
            raise ValueError("Missed outputs")


@dataclass
class OperationRequirement:
    """
    Operation requirement for a Core to be compliant

    Author: Nicola Ricciardi
    """

    constraint: Constraint
    override_sink: Optional[EventHandler] = field(default=None, kw_only=True)
    default_setup_data: Optional[bytes] = field(default=None, kw_only=True)

    @property
    def has_override_sink(self) -> bool:
        return self.override_sink is not None
