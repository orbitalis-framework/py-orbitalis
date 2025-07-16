from dataclasses import dataclass, field
from typing import Optional, Set, Dict, List, Any, TypeVar, Generic, Self

from dataclasses_avroschema import AvroModel

from busline.client.subscriber.topic_subscriber.event_handler.event_handler import EventHandler
from orbitalis.orbiter.schemaspec import SchemaSpec, Input, Output, InputsOutputs
from orbitalis.utils.allowblocklist import AllowBlockListMixin


@dataclass(kw_only=True)
class Constraint(AllowBlockListMixin, InputsOutputs):
    """
    min: minimum number (mandatory excluded)
    max: maximum number (mandatory excluded)
    mandatory: identifiers
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


SetupData = TypeVar('SetupData', bound=AvroModel)


@dataclass()
class Need(Generic[SetupData]):
    constraint: Constraint
    override_sink: Optional[EventHandler] = field(default=None)
    setup_data: Optional[SetupData] = field(default=None, kw_only=True)

    @property
    def has_override_sink(self) -> bool:
        return self.override_sink is not None


