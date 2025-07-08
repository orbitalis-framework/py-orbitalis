from __future__ import annotations

import logging
from typing import override, Dict, List
from abc import ABC
from dataclasses import dataclass, field

from dataclasses_avroschema import AvroModel
from datetime import datetime
from busline.client.subscriber.topic_subscriber.event_handler import schemafull_event_handler
from busline.event.event import Event
from orbitalis.core.descriptor import CoreDescriptor
from orbitalis.events.handshake.discover import DiscoverMessage
from orbitalis.events.handshake.offer import OfferMessage, OfferedOperation
from orbitalis.events.handshake.reply import RequestMessage, RejectMessage
from orbitalis.events.wellknown_topic import WellKnownHandShakeTopic
from orbitalis.core.need import ConstrainedNeed
from orbitalis.orb.orbiter import Orbiter
from orbitalis.plugin.operation import Operation
from orbitalis.plugin.state import PluginState
from orbitalis.state_machine.state_machine import StateMachine



@dataclass(kw_only=True)
class Plugin(Orbiter, StateMachine, ABC):
    """

    Author: Nicola Ricciardi
    """

    operations: Dict[str, Operation] = field(default_factory=dict)     # operation_name => Operation

    pending_cores: Dict[str, Dict[str, datetime]] = field(default_factory=dict, init=False)     # core_identifier => { operation_name => when }
    core_descriptors: Dict[str, CoreDescriptor] = field(default_factory=dict, init=False)       # core_identifier => CoreDescriptor

    # === CONFIGURATION parameters ===
    discover_topic: str = field(default_factory=lambda: WellKnownHandShakeTopic.discover_topic())

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

        await self.eventbus_client.subscribe(self.discover_topic)

    @schemafull_event_handler(AvroModel.avro_schema_to_python(DiscoverMessage))
    async def discover_event_handler(self, topic: str, event: Event[DiscoverMessage]):
        # TODO: use priority to disconnect from a low priority core, if high priority core incomes

        logging.info(f"{self}: new discover event from {event.payload.core_identifier}")

        offerable_operations: List[str] = []

        for need_operation_name, need in event.payload.needs.items():
            if need_operation_name in self.operations:
                if not self.operations[need_operation_name].policy.is_compliance(event.payload.core_identifier):
                    continue

                if len(self.operations[need_operation_name].associated_cores) < self.operations[need_operation_name].policy.maximum:
                    offerable_operations.append(need_operation_name)

                # TODO:
                # - remove low priority cores
                # - consider high priority cores in pending status during computation of available slots


        if len(offerable_operations) > 0:
            await self.send_offer(
                event.payload.offer_topic,
                event.payload.core_identifier,
                offerable_operations
            )


    def build_reply_topic(self, core_identifier: str) -> str:
        return WellKnownHandShakeTopic.build_reply_topic(core_identifier, self.identifier)

    def build_operation_topic_for_core(self, core_identifier: str, operation_name: str) -> str:
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
                    self.build_operation_topic_for_core(core_identifier, operation_name),
                    self.operations[operation_name].handler.input_schemas()
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

    @schemafull_event_handler([
        AvroModel.avro_schema_to_python(RequestMessage),
        AvroModel.avro_schema_to_python(RejectMessage)
    ])
    async def reply_event_handler(self, topic: str, event: Event[RequestMessage | RejectMessage]):
        logging.info(f"{self}: new reply")

        # core_identifier: str = event.payload.core_identifier
        #
        # if core_identifier not in self.context.pending_requests:
        #     logging.debug(
        #         f"{self.context}: {core_identifier} sent a ReplyMessage, but there is no related pending request")
        #     return
        #
        # if not event.payload.plug_request:
        #     logging.debug(f"{self.context}: plug request False; remove {core_identifier} from pending requests")
        #     self.context.pending_requests.pop(core_identifier, None)
        #     return
        #
        # logging.debug(f"{self.context}: {core_identifier} confirm plug request")
        #
        # plug_confirmed: bool = self.context.can_plug_into_core(
        #     self.context.pending_requests[core_identifier].core_descriptor)
        #
        # logging.debug(f"{self.context}: can plug to {core_identifier}? {plug_confirmed}")
        #
        # if plug_confirmed:
        #     await self.context.plug_into_core(self.context.pending_requests[core_identifier].core_descriptor)
        #
        # await self.context.eventbus_client.publish(
        #     event.payload.response_topic,
        #     ResponseMessage(
        #         plugin_identifier=self.context.identifier,
        #         plug_confirmed=plug_confirmed
        #     ).into_event()
        # )



