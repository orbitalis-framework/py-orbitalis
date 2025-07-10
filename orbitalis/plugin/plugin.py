from __future__ import annotations

import logging
from collections import defaultdict
from typing import override, Dict, List, Tuple
from abc import ABC
from dataclasses import dataclass, field

from datetime import datetime
from busline.client.subscriber.topic_subscriber.event_handler import schemafull_event_handler
from busline.event.event import Event
from orbitalis.core.descriptor import CoreDescriptor
from orbitalis.events.discover import DiscoverMessage
from orbitalis.events.offer import OfferMessage, OfferedOperation
from orbitalis.events.reply import RequestMessage, RejectMessage
from orbitalis.events.response import ResponseMessage
from orbitalis.events.wellknown_topic import WellKnownTopic
from orbitalis.orb.orbiter import Orbiter
from orbitalis.plugin.operation import Operation
from orbitalis.plugin.state import PluginState
from orbitalis.state_machine.state_machine import StateMachine


@dataclass(frozen=True)
class Connection:
    operation_name: str
    core_identifier: str
    input_topic: str
    result_topic: str
    when: datetime = field(default_factory=lambda: datetime.now())


@dataclass(kw_only=True)
class Plugin(Orbiter, StateMachine, ABC):
    """

    Author: Nicola Ricciardi
    """

    operations: Dict[str, Operation] = field(default_factory=dict)     # operation_name => Operation

    pending_cores: Dict[str, Dict[str, datetime]] = field(default_factory=dict, init=False)     # core_identifier => { operation_name => when }
    lent_operations: Dict[str, Dict[str, Connection]] = field(default_factory=lambda: defaultdict(dict), init=False)     # operation_name => { core_identifier => Connection }

    # === CONFIGURATION parameters ===
    discover_topic: str = field(default_factory=lambda: WellKnownTopic.discover_topic())

    def __post_init__(self):
        self.state = PluginState.CREATED

    @override
    async def _internal_start(self, *args, **kwargs):
        await super()._internal_start(*args, **kwargs)

        await self.subscribe_on_discover()
        self.state = PluginState.RUNNING


    async def subscribe_on_discover(self):
        # TODO: gestione eccezioni
        # TODO: gestione on_event

        await self.eventbus_client.subscribe(self.discover_topic, self.discover_event_handler)

    def can_lend_to_core(self, core_identifier: str, operation_name: str) -> bool:
        if not self.operations[operation_name].policy.is_compliance(core_identifier):
            return False

        if len(self.lent_operations[operation_name].keys()) < self.operations[operation_name].policy.maximum:
            return True

        return False

    @schemafull_event_handler(DiscoverMessage.avro_schema_to_python())
    async def discover_event_handler(self, topic: str, event: Event[DiscoverMessage]):
        logging.info(f"{self}: new discover event from {event.payload.core_identifier}: {topic} -> {event}")

        offerable_operations: List[str] = []

        for needed_operation_name, needed_operation_information in event.payload.needed_operations.items():
            if needed_operation_name in self.operations:
                if needed_operation_information.blocklist is not None and self.identifier in needed_operation_information.blocklist:
                    continue

                if self.can_lend_to_core(event.payload.core_identifier, needed_operation_name):
                    offerable_operations.append(needed_operation_name)

                # TODO:
                # - remove low priority cores
                # - consider high priority cores in pending status during computation of available slots

        logging.debug(f"{self}: send offer for these operations: {offerable_operations}")
        if len(offerable_operations) > 0:
            await self.send_offer(
                event.payload.offer_topic,
                event.payload.core_identifier,
                offerable_operations
            )


    def build_reply_topic(self, core_identifier: str) -> str:
        return WellKnownTopic.build_reply_topic(core_identifier, self.identifier)

    def build_operation_input_topic_for_core(self, core_identifier: str, operation_name: str) -> str:
        return f"{operation_name}.{core_identifier}.{self.identifier}"

    async def send_offer(self, offer_topic: str, core_identifier: str, offerable_operations: List[str]):

        if len(offerable_operations) == 0:
            return

        reply_topic = self.build_reply_topic(core_identifier)

        await self.eventbus_client.subscribe(
            topic=reply_topic,
            handler=self.reply_event_handler
        )

        offered_operations: List[OfferedOperation] = []

        for operation_name in offerable_operations:
            offered_operations.append(
                OfferedOperation(
                    operation_name,
                    self.operations[operation_name].handler.schemas_fingerprints()
                )
            )

        await self.eventbus_client.publish(
            topic=offer_topic,
            event=OfferMessage(
                plugin_identifier=self.identifier,
                offered_operations=offered_operations,
                reply_topic=reply_topic
            ).into_event()
        )

        for operation_name in offerable_operations:
            self.pending_cores[core_identifier][operation_name] = datetime.now()

    def build_response_topic(self, core_identifier: str) -> str:
        return WellKnownTopic.build_response_topic(core_identifier, self.identifier)

    async def reject_event_handler(self, topic: str, event: Event[RejectMessage]):
        logging.debug(f"{self}: core {event.payload.core_identifier} rejects plug request for these operations: {event.payload.rejected_operations}")

        for operation_name in event.payload.rejected_operations:
            self.pending_cores[event.payload.core_identifier].pop(operation_name, None)

    async def setup_and_register_to_core(self, core_identifier: str, lent_operations: Dict[str, Tuple[str, str]], lent_denied_operations: List[str]):
        """
        lent_operations: operation_name => (input_topic, result_topic)

        1. Subscribe to result topic, i.e. lent_operations[operation_name][1]
        2. Sent Response to core
        3. Remove pending core requests
        """

        try:
            for lent_operation_name, input_topic, result_topic in lent_operations.items():
                await self.eventbus_client.subscribe(input_topic, self.operations[lent_operation_name].handler)

            await self.eventbus_client.publish(
                self.build_response_topic(core_identifier),
                ResponseMessage(
                    plugin_identifier=self.identifier,
                    operations=dict([(lent_operation_name, input_topic) for lent_operation_name, input_topic, result_topic in lent_operations.items()]),
                    denied_operations=lent_denied_operations
                ).into_event()
            )

            for operation_name in lent_denied_operations:
                if operation_name not in self.pending_cores[core_identifier]:
                    logging.warning(f"{self}: operation without no pending request")
                    continue

                del self.pending_cores[core_identifier][operation_name]

            for operation_name, input_topic, result_topic in lent_operations.items():
                if operation_name not in self.pending_cores[core_identifier]:
                    logging.warning(f"{self}: operation without no pending request")
                    continue

                self.lent_operations[operation_name][core_identifier] = Connection(
                    core_identifier=core_identifier,
                    operation_name=operation_name,
                    input_topic=input_topic,
                    result_topic=result_topic
                )

                del self.pending_cores[core_identifier][operation_name]


        except Exception as e:
            logging.error(f"{self}: {e}")

            await self.eventbus_client.multi_unsubscribe([input_topic for input_topic, result_topic in lent_operations.values()])


    async def request_event_handler(self, topic: str, event: Event[RequestMessage]):
        logging.debug(f"{self}: core {event.payload.core_identifier} confirms plug request for these operations: {event.payload.requested_operations}")

        operations_to_lend: List[str] = []
        lend_denied_operations: List[str] = []
        for operation_name in event.payload.requested_operations:
            plug_confirmed: bool = self.can_lend_to_core(event.payload.core_identifier, operation_name)

            logging.debug(f"{self}: can lend to {event.payload.core_identifier}? {plug_confirmed}")

            if plug_confirmed:
                operations_to_lend.append(operation_name)
            else:
                lend_denied_operations.append(operation_name)

        lent_operations = {}
        for operation_name in operations_to_lend:
            lent_operations[operation_name] = (
                self.build_operation_input_topic_for_core(event.payload.core_identifier, operation_name),  # input_topic
                event.payload.requested_operations[operation_name]  # result_topic
            )

        await self.setup_and_register_to_core(
            event.payload.core_identifier,
            lent_operations,
            lend_denied_operations
        )

    @schemafull_event_handler([
        RequestMessage.avro_schema_to_python(),
        RejectMessage.avro_schema_to_python()
    ])
    async def reply_event_handler(self, topic: str, event: Event[RequestMessage | RejectMessage]):
        logging.info(f"{self}: new reply: {topic} -> {event}")

        if event.payload.core_identifier not in self.pending_cores:
            logging.debug(f"{self}: {event.payload.core_identifier} sent a reply, but there is no related pending request")
            return

        if isinstance(event.payload, RequestMessage):
            await self.request_event_handler(topic, event)

        elif isinstance(event.payload, RejectMessage):
            await self.reject_event_handler(topic, event)

        else:
            raise ValueError("Unexpected reply payload")





