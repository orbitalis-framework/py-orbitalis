from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, Optional, List, TypeVar, Generic

from dataclasses_avroschema import AvroModel

from busline.event.registry import registry
from busline.event.avro_payload import AvroEventPayload
from orbitalis.core.need import Constraint
from orbitalis.utils.allowblocklist import AllowBlockListMixin


D = TypeVar('D', bound=AvroModel)


@dataclass(frozen=True, kw_only=True)
@registry
class GracelessCloneConnectionMessage(AvroEventPayload, Generic[D]):
    """
    Orbiter A --- close ---> Orbiter B

    TODO

    Author: Nicola Ricciardi
    """

    from_identifier: str
    operation_name: str
    data: Optional[D]


@dataclass(frozen=True, kw_only=True)
@registry
class GracefulCloneConnectionMessage(AvroEventPayload, Generic[D]):
    """
    Orbiter A --- close ---> Orbiter B

    TODO

    Author: Nicola Ricciardi
    """

    from_identifier: str
    operation_name: str
    ack_topic: str
    data: Optional[D]


@dataclass(frozen=True, kw_only=True)
@registry
class CloseConnectionAckMessage(AvroEventPayload, Generic[D]):
    """
    Orbiter A <--- close ack --- Orbiter B

    TODO

    Author: Nicola Ricciardi
    """

    from_identifier: str
    operation_name: str

