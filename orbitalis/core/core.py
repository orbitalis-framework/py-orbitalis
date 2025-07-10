import random
from collections import defaultdict
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
from orbitalis.events.wellknown_topic import WellKnownTopic
from orbitalis.core.need import Need, ConstrainedNeed
from orbitalis.orb.orbiter import Orbiter

from orbitalis.plugin.descriptor import PluginDescriptor
from orbitalis.state_machine.state_machine import StateMachine


@dataclass(frozen=True)
class PendingRequest:
    operation_name: str
    result_topic: str
    when: datetime = field(default_factory=lambda: datetime.now())



@dataclass(kw_only=True)
class Core(Orbiter, StateMachine[CoreState]):

    # TODO: unify operation_plugin_topic and operation_needs

    operation_plugin_topic: Dict[str, Dict[str, str]] = field(default_factory=lambda: defaultdict(dict), init=False)     # operation_name => { plugin_identifier => topic }
    pending_plugins: Dict[str, Dict[str, PendingRequest]] = field(default_factory=dict, init=False)       # plugin_identifier => { operation_name => when }
    plugin_descriptors: Dict[str, PluginDescriptor] = field(default_factory=dict, init=False)       # plugin_identifier => PluginDescriptor

    # === CONFIGURATION parameters ===
    discover_topic: str = field(default_factory=lambda: WellKnownTopic.discover_topic())
    discovering_interval: float = field(default=2)
    needed_operations: Dict[str, ConstrainedNeed] = field(default_factory=dict)  # operation_name => Need

    def __post_init__(self):
        self.state = CoreState.CREATED

    @property
    def keepalive_topic(self) -> str:
        return WellKnownTopic.build_keepalive_topic(self.identifier)

    @property
    def general_purpose_hook_topic(self) -> str:
        return WellKnownTopic.build_keepalive_topic(self.identifier)


    def _need_for_operation(self, operation_name: str) -> Optional[Need]:
        """

        :return: None is there are no needs
        """

        if operation_name not in self.needed_operations.keys():
            return None

        need = self.needed_operations[operation_name].to_need()

        for plugin_identifier in self.operation_plugin_topic[operation_name].keys():
            need.mandatory.remove(plugin_identifier)

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

        for operation_name in self.needed_operations.keys():
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
        for operation_name in self.needed_operations.keys():
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
        return WellKnownTopic.build_response_topic(
                    self.identifier,
                    plugin_identifier
                )

    def build_operation_result_topic(self, plugin_identifier: str, operation_name: str) -> str:
        return f"{operation_name}.{self.identifier}.{plugin_identifier}.result"

    @schemafull_event_handler(OfferMessage.avro_schema_to_python())
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

            operations_to_request: Dict[str, str] = dict([(operation_name, self.build_operation_result_topic(event.payload.plugin_identifier, operation_name)) for operation_name in operations_to_request])

            await self.eventbus_client.publish(
                topic=event.payload.reply_topic,
                event=RequestMessage(
                    core_identifier=self.identifier,
                    core_keepalive_topic=self.keepalive_topic,
                    core_general_purpose_hook=self.general_purpose_hook_topic,
                    response_topic=response_topic,
                    requested_operations=operations_to_request
                ).into_event()
            )

            for operation_name, result_topic in operations_to_request.items():
                self.pending_plugins[event.payload.plugin_identifier][operation_name] = PendingRequest(operation_name, result_topic)

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


    @schemafull_event_handler(ResponseMessage.avro_schema_to_python())
    async def response_event_handler(self, topic: str, event: Event[ResponseMessage]):
        logging.debug(f"{self}: new response: {topic} -> {event}")

        # TODO


    @override
    async def _internal_start(self, *args, **kwargs):
        await super()._internal_start(*args, **kwargs)

        self.update_compliance()

        if self.state == CoreState.NOT_COMPLIANT:
            await self.send_discover()

    @override
    async def _internal_stop(self, *args, **kwargs):
        await super()._internal_stop(*args, **kwargs)

        topics: List[str] = [
            self.discover_topic,
        ]

        await self.eventbus_client.multi_unsubscribe(topics, parallelize=True)


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


        await self.eventbus_client.multi_publish(list(topics), payload.into_event() if payload is not None else None)


    async def sudo_execute(self, topic: str, payload: Optional[AvroEventPayload] = None):
        await self.eventbus_client.publish(topic, payload.into_event() if payload is not None else None)



