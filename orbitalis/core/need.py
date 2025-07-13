from dataclasses import dataclass, field
from typing import Optional, Set, Dict, List, Any, TypeVar, Generic, Self

from dataclasses_avroschema import AvroModel

from orbitalis.orbiter.schemaspec import SchemaSpec, InputOutputSchemaSpec
from orbitalis.utils.allowblocklist import AllowBlockListMixin


@dataclass(kw_only=True)
class Constraint(AllowBlockListMixin, InputOutputSchemaSpec):
    """
    min: minimum number (mandatory excluded)
    max: maximum number (mandatory excluded)
    mandatory: identifiers
    """

    minimum: int = field(default=0)
    maximum: Optional[int] = field(default=None)
    mandatory: Optional[List[str]] = field(default=None)


    def __post_init__(self):
        if self.minimum < 0 or (self.maximum is not None and self.maximum < 0) \
                or (self.maximum is not None and self.minimum > self.maximum):
            raise ValueError("minimum and/or maximum value are invalid")


SetupData = TypeVar('SetupData', bound=AvroModel)


@dataclass()
class Need(Generic[SetupData]):
    constraint: Constraint
    setup_data: Optional[SetupData] = field(default=None, kw_only=True)



