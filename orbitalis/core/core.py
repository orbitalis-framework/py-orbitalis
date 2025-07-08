import random
from datetime import datetime
import logging
from dataclasses import dataclass, field
from typing import override, Dict, Set, Optional, List

from dataclasses_avroschema import AvroModel

from busline.client.subscriber.topic_subscriber.event_handler import schemafull_event_handler
from busline.event.avro_payload import AvroEventPayload
from busline.event.event import Event
from orbitalis.core.state import CoreState
from orbitalis.events.handshake.discover import DiscoverMessage
from orbitalis.events.handshake.offer import OfferMessage, OfferedOperation
from orbitalis.events.handshake.reply import RequestMessage, RejectMessage
from orbitalis.events.handshake.response import ResponseMessage
from orbitalis.events.wellknown_topic import WellKnownHandShakeTopic
from orbitalis.core.need import Need, ConstrainedNeed
from orbitalis.orb.orbiter import Orbiter

from orbitalis.plugin.descriptor import PluginDescriptor
from orbitalis.state_machine.state_machine import StateMachine


@dataclass(kw_only=True)
class Core(Orbiter, StateMachine[CoreState]):

    # TODO: unify operation_plugin_topic and operation_needs

    operation_plugin_topic: Dict[str, Dict[str, str]] = field(default_factory=dict, init=False)     # operation_name => { plugin_identifier => topic }
    pending_plugins: Dict[str, Dict[str, datetime]] = field(default_factory=dict, init=False)       # plugin_identifier => { operation_name => when }
    plugin_descriptors: Dict[str, PluginDescriptor] = field(default_factory=dict, init=False)       # plugin_identifier => PluginDescriptor

    # === CONFIGURATION parameters ===
    discover_topic: str = field(default_factory=lambda: WellKnownHandShakeTopic.discover_topic())
    discovering_interval: float = field(default=2)
    operation_needs: Dict[str, ConstrainedNeed] = field(default_factory=dict)  # operation_name => Need

    def __post_init__(self):
        self.state = CoreState.CREATED


    def _need_for_operation(self, operation_name: str) -> Optional[Need]:
        """

        :return: None is there are no needs
        """

        if operation_name not in self.operation_needs.keys():
            return None

        need = self.operation_needs[operation_name].to_need()

        for plugin_identifier in self.operation_plugin_topic[operation_name].keys():
            need.mandatory.discard(plugin_identifier)

            if need.maximum is not None:
                need.maximum = max(0, need.maximum - 1)

            need.minimum = max(0, need.minimum - 1)

        if need.maximum is not None and need.maximum == 0:
            return None

        return need


    def is_compliance_for_operation(self, operation_name: str) -> bool:
        """
        Return True if the plugged plugins are enough to perform given service
        """

        return self._need_for_operation(operation_name) is None

    def is_compliance(self) -> bool:
        """
        Return True if the plugged plugins are enough to perform all services
        """

        for operation_name in self.operation_needs.keys():
            if not self.is_compliance_for_operation(operation_name):
                return False

        return True

    def update_compliance(self):

        if self.is_compliance():
            self.state = CoreState.COMPLIANT
        else:
            self.state = CoreState.NOT_COMPLIANT


    def build_offer_topic(self) -> str:
        return f"$handshake.{self.identifier}.offer"

    async def send_discover(self):

        offer_topic = self.build_offer_topic()

        needs = {}
        for operation_name in self.operation_needs.keys():
            needs[operation_name] = self._need_for_operation(operation_name)

        if len(needs) == 0:
            return

        await self.eventbus_client.subscribe(
            topic=offer_topic,
            handler=self.offer_event_handler
        )

        await self.eventbus_client.publish(
            self.discover_topic,
            DiscoverMessage(
                core_identifier=self.identifier,
                needs=needs,
                offer_topic=offer_topic
            ).into_event()
        )

    def is_plugin_operation_needed_and_pluggable(self, plugin_identifier: str, offered_operation: OfferedOperation) -> bool:
        return False        # TODO

    def build_response_topic(self, plugin_identifier: str) -> str:
        return WellKnownHandShakeTopic.build_response_topic(
                    self.identifier,
                    plugin_identifier
                )

    @schemafull_event_handler(AvroModel.avro_schema_to_python(OfferMessage))
    async def offer_event_handler(self, topic: str, event: Event[OfferMessage]):
        logging.debug(f"{self}: new offer: {topic} -> {event}")

        operations_to_request: List[str] = []
        operations_to_reject: List[str] = []
        for offered_operation in event.payload.offered_operations:
            if self.is_plugin_operation_needed_and_pluggable(event.payload.plugin_identifier, offered_operation):
                operations_to_request.append(offered_operation.operation_name)
            else:
                operations_to_reject.append(offered_operation.operation_name)


        response_topic: str = self.build_response_topic(event.payload.plugin_identifier)

        if len(operations_to_request) > 0:
            logging.debug(f"{self}: operations to request: {operations_to_request}")

            await self.eventbus_client.subscribe(
                topic=response_topic,
                handler=self.response_event_handler
            )

            await self.eventbus_client.publish(
                topic=event.payload.reply_topic,
                event=RequestMessage(
                    core_descriptor=...,  # TODO
                    response_topic=response_topic,
                    requested_operations=...  # TODO
                ).into_event()
            )

            # TODO: save in pending topics; solve TODO row 28

            for operation_name in operations_to_request:
                self.pending_plugins[event.payload.plugin_identifier][operation_name] = datetime.now()

        if len(operations_to_reject) > 0:
            logging.debug(f"{self}: operations to reject: {operations_to_reject}")

            await self.eventbus_client.publish(
                topic=event.payload.reply_topic,
                event=RejectMessage(
                    core_identifier=self.identifier,
                    description="Not needed anymore, sorry",
                    rejected_operations=operations_to_reject
                ).into_event()
            )


    @schemafull_event_handler(AvroModel.avro_schema_to_python(ResponseMessage))
    async def response_event_handler(self, topic: str, event: Event[ResponseMessage]):
        logging.debug(f"{self}: new response: {topic} -> {event}")

        # TODO


    @override
    async def _internal_start(self, *args, **kwargs):
        await super()._internal_start(*args, **kwargs)

        self.update_compliance()

        if self.state == CoreState.NOT_COMPLIANT:
            await self.send_discover()



    async def execute(self, operation_name: str, payload: Optional[AvroEventPayload] = None,
        /, any: bool = False, all: bool = False, plugin_identifier: Optional[str] = None):
        """
        Execute the operation by its name.

        You must specify which plugin must be used, otherwise ValueError is raised.
        """

        topics: Set[str] = set()

        if plugin_identifier is not None:

            if plugin_identifier not in self.plugin_descriptors:
                raise ValueError("plugin not plugged")

            if operation_name not in self.plugin_descriptors[plugin_identifier].operations:
                raise ValueError(f"plugin has not operation {operation_name} (or it has not share it with this core {self.identifier})")

            if payload is not None and self.plugin_descriptors[plugin_identifier].operations[operation_name] != payload:
                raise ValueError("incompatible input payload schema and plugin operation")

            topics.add(self.operation_plugin_topic[operation_name][plugin_identifier])

        elif all or any:
            for plugin_identifier, topic in self.operation_plugin_topic[operation_name].items():
                if payload is not None and self.plugin_descriptors[plugin_identifier].operations[operation_name] != payload:
                    continue

                topics.add(topic)

            if any:
                topics = { random.choice(list(topics)) }

        else:
            raise ValueError("modality (any/all/identifier) must be specified")


        await self.eventbus_client.multi_publish(list(topics), payload.into_event())






