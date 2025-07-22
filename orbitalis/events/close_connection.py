from dataclasses import dataclass
from typing import Optional, TypeVar, Generic

from dataclasses_avroschema import AvroModel

from busline.event.registry import add_to_registry
from busline.event.message.avro_message import AvroMessageMixin

D = TypeVar('D', bound=AvroModel)


@dataclass(frozen=True, kw_only=True)
@add_to_registry
class GracelessCloneConnectionMessage(AvroMessageMixin, Generic[D]):
    """
    Orbiter A --- close ---> Orbiter B

    TODO

    Author: Nicola Ricciardi
    """

    from_identifier: str
    operation_name: str
    data: Optional[D]


@dataclass(frozen=True, kw_only=True)
@add_to_registry
class GracefulCloneConnectionMessage(AvroMessageMixin, Generic[D]):
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
@add_to_registry
class CloseConnectionAckMessage(AvroMessageMixin, Generic[D]):
    """
    Orbiter A <--- close ack --- Orbiter B

    TODO

    Author: Nicola Ricciardi
    """

    from_identifier: str
    operation_name: str

