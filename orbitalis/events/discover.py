from dataclasses import dataclass
from typing import Dict

from dataclasses_avroschema import AvroModel

from busline.event.registry import add_to_registry
from busline.event.message.avro_message import AvroMessageMixin
from orbitalis.core.requirement import Constraint
from orbitalis.orbiter.schemaspec import Outputs, Inputs
from orbitalis.utils.allowblocklist import AllowBlockListMixin


@dataclass
class DiscoverQuery(AllowBlockListMixin, Inputs, Outputs, AvroModel):

    operation_name: str

    @classmethod
    def from_constraint(cls, operation_name: str, constraint: Constraint):
        return cls(
            operation_name=operation_name,
            inputs=constraint.inputs,
            outputs=constraint.outputs,
            allowlist=constraint.allowlist,
            blocklist=constraint.blocklist,
        )



@dataclass(frozen=True)
class DiscoverMessage(AvroMessageMixin):
    """
    Core --- discover ---> Plugin

    Message used by cores to notify plugins of their presence and to ask operations connections,
    core provides a full set of pluggable operations with related information

    Author: Nicola Ricciardi
    """

    core_identifier: str
    offer_topic: str
    core_keepalive_topic: str
    core_keepalive_request_topic: str
    considered_dead_after: float
    queries: Dict[str, DiscoverQuery]   # operation_name => DiscoverQuery
