from __future__ import annotations

import logging
from collections import defaultdict
from typing import override, Dict, List, Tuple, Optional
from abc import ABC
from dataclasses import dataclass, field

from datetime import datetime
from busline.client.subscriber.topic_subscriber.event_handler import schemafull_event_handler, event_handler
from busline.event.event import Event
from orbitalis.events.discover import DiscoverMessage
from orbitalis.events.offer import OfferMessage, OfferedOperation
from orbitalis.events.reply import RequestMessage, RejectMessage
from orbitalis.events.response import ResponseMessage
from orbitalis.events.wellknown_topic import WellKnownTopic
from orbitalis.orbiter.orbiter import Orbiter
from orbitalis.orbiter.pending_request import PendingRequest
from orbitalis.plugin.operation import Operation
from orbitalis.plugin.state import PluginState
from orbitalis.state_machine.state_machine import StateMachine


@dataclass(kw_only=True)
class Plugin(Orbiter, StateMachine, ABC):
    """

    TODO: a way to provide dynamic policies for operations

    Author: Nicola Ricciardi
    """


    operations: Dict[str, Operation] = field(default_factory=dict, init=False)     # operation_name => Operation

    def __post_init__(self):
        self.state = PluginState.CREATED

        # used to refresh operations
        for attr_name in dir(self):
            _ = getattr(self, attr_name)


    @override
    async def _internal_start(self, *args, **kwargs):
        await super()._internal_start(*args, **kwargs)

        await self.subscribe_on_discover()
        self.state = PluginState.RUNNING


    async def subscribe_on_discover(self):
        await self.eventbus_client.subscribe(self.discover_topic, self.discover_event_handler)

    def can_lend_to_core(self, core_identifier: str, operation_name: str) -> bool:
        if not self.operations[operation_name].policy.is_compliance(core_identifier):
            return False

        if self.operations[operation_name].policy.maximum is None or len(self.retrieve_connections(operation_name=operation_name)) < self.operations[operation_name].policy.maximum:
            return True

        return False

    @event_handler
    async def discover_event_handler(self, topic: str, event: Event[DiscoverMessage]):
        logging.info(f"{self}: new discover event from {event.payload.core_identifier}: {topic} -> {event}")

        offerable_operations: List[str] = []

        for core_needed_operation_name, core_needed_operation_information in event.payload.needed_operations.items():
            if core_needed_operation_name in self.operations:

                # check compatibility with block/allow list
                if (core_needed_operation_information.blocklist is not None and self.identifier in core_needed_operation_information.blocklist) \
                        or (core_needed_operation_information.allowlist is not None and self.identifier not in core_needed_operation_information.allowlist):
                    continue

                # check if already in pending request
                if event.payload.core_identifier in self.pending_requests.keys() \
                        and core_needed_operation_name in self.pending_requests_by_remote_identifier(event.payload.core_identifier):
                    continue

                # check if this plugin have already lent operation to core
                if len(self.retrieve_connections(remote_identifier=event.payload.core_identifier, operation_name=core_needed_operation_name)) > 0:
                    continue

                # check if there are slot available
                if self.operations[core_needed_operation_name].policy.maximum is not None:
                    current_reserved_slot_for_operation: int = len(self.retrieve_connections(operation_name=core_needed_operation_name))

                    for core_identifier, operations in self.pending_requests.items():
                        if core_needed_operation_name in operations.keys():
                            current_reserved_slot_for_operation += 1

                    if current_reserved_slot_for_operation >= self.operations[core_needed_operation_name].policy.maximum:
                        continue

                # check input_schemas compatibility
                if not self.operations[core_needed_operation_name].input.is_compatible(core_needed_operation_information.input):
                    continue

                # check output_schemas compatibility
                if core_needed_operation_information.has_output:
                    if not self.operations[core_needed_operation_name].has_output:
                        continue

                    if not core_needed_operation_information.output.is_compatible(self.operations[core_needed_operation_name].output):
                        continue

                if self.can_lend_to_core(event.payload.core_identifier, core_needed_operation_name):
                    offerable_operations.append(core_needed_operation_name)

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
        return f"{operation_name}.{core_identifier}.{self.identifier}.input"

    async def send_offer(self, offer_topic: str, core_identifier: str, offerable_operations: List[str]):

        if len(offerable_operations) == 0:
            return

        offered_operations: List[OfferedOperation] = []

        for operation_name in offerable_operations:
            offered_operations.append(
                OfferedOperation(
                    operation_name=operation_name,
                    operation_input_schemas=self.operations[operation_name].input_schemas,
                    operation_output_schemas=self.operations[operation_name].output_schemas
                )
            )

        reply_topic = self.build_reply_topic(core_identifier)

        for operation_name in offerable_operations:
            self.pending_requests_by_remote_identifier(core_identifier)[operation_name] = PendingRequest(
                operation_name=operation_name,
                remote_identifier=core_identifier,
                offer_topic=offer_topic,
                reply_topic=reply_topic
            )

        try:

            await self.eventbus_client.subscribe(
                topic=reply_topic,
                handler=self.reply_event_handler
            )

            await self.eventbus_client.publish(
                topic=offer_topic,
                event=OfferMessage(
                    plugin_identifier=self.identifier,
                    offered_operations=offered_operations,
                    reply_topic=reply_topic
                ).into_event()
            )
        except Exception as e:
            logging.error(f"{self}: {e}")

            for operation_name in offerable_operations:
                del self.pending_requests_by_remote_identifier(core_identifier)[operation_name]

    # async def reject_event_handler(self, topic: str, event: Event[RejectMessage]):
    #     logging.debug(f"{self}: core {event.payload.core_identifier} rejects plug request for these operations: {event.payload.rejected_operations}")
    #
    #     for operation_name in event.payload.rejected_operations:
    #         self.pending_requests_by_remote_identifier(event.payload.core_identifier).pop(operation_name, None)

    # async def setup_and_register_to_core(self, response_topic: str, core_identifier: str, lent_operations: Dict[str, Tuple[str, str]], lent_denied_operations: List[str]):
    #     """
    #     lent_operations: operation_name => (input_topic, result_topic)
    #
    #     1. Subscribe to result topic, i.e. lent_operations[operation_name][1]
    #     2. Sent Response to core
    #     3. Remove pending core requests
    #     """
    #
    #     try:
    #         for lent_operation_name, (input_topic, result_topic) in lent_operations.items():
    #             await self.eventbus_client.subscribe(input_topic, self.operations[lent_operation_name].handler)
    #     except Exception as e:
    #         logging.error(f"{self}: {e}")
    #         return
    #
    #     try:
    #         await self.eventbus_client.publish(
    #             response_topic,
    #             ResponseMessage(
    #                 plugin_identifier=self.identifier,
    #                 operations=dict([(lent_operation_name, input_topic) for lent_operation_name, (input_topic, result_topic) in lent_operations.items()]),
    #                 denied_operations=lent_denied_operations
    #             ).into_event()
    #         )
    #
    #         # TODO
    #         # for operation_name in lent_denied_operations:
    #         #     if operation_name not in self.pending_cores[core_identifier]:
    #         #         logging.warning(f"{self}: operation without no pending request")
    #         #         continue
    #         #
    #         #     del self.pending_cores[core_identifier][operation_name]
    #         #
    #         # for operation_name, input_topic, result_topic in lent_operations.items():
    #         #     if operation_name not in self.pending_cores[core_identifier]:
    #         #         logging.warning(f"{self}: operation without no pending request")
    #         #         continue
    #         #
    #         #     self.lent_operations[operation_name][core_identifier] = _Connection(
    #         #         core_identifier=core_identifier,
    #         #         operation_name=operation_name,
    #         #         input_topic=input_topic,
    #         #         output_topic=result_topic
    #         #     )
    #         #
    #         #     del self.pending_cores[core_identifier][operation_name]
    #
    #
    #     except Exception as e:
    #         logging.error(f"{self}: {e}")
    #
    #         await self.eventbus_client.multi_unsubscribe([input_topic for input_topic, result_topic in lent_operations.values()])


    # async def request_event_handler(self, topic: str, event: Event[RequestMessage]):
    #     logging.debug(f"{self}: core {event.payload.core_identifier} confirms plug request for these operations: {event.payload.requested_operations}")
    #
    #     operations_to_lend: List[str] = []
    #     lend_denied_operations: List[str] = []
    #     for operation_name in event.payload.requested_operations:
    #         plug_confirmed: bool = self.can_lend_to_core(event.payload.core_identifier, operation_name)
    #
    #         logging.debug(f"{self}: can lend to {event.payload.core_identifier}? {plug_confirmed}")
    #
    #         if plug_confirmed:
    #             operations_to_lend.append(operation_name)
    #         else:
    #             lend_denied_operations.append(operation_name)
    #
    #     lent_operations = {}
    #     for operation_name in operations_to_lend:
    #         lent_operations[operation_name] = (
    #             self.build_operation_input_topic_for_core(event.payload.core_identifier, operation_name),  # input_topic
    #             event.payload.requested_operations[operation_name]  # output_topic
    #         )
    #
    #     await self.setup_and_register_to_core(
    #         event.payload.response_topic,
    #         event.payload.core_identifier,
    #         lent_operations,
    #         lend_denied_operations
    #     )

    # @event_handler
    # async def reply_event_handler(self, topic: str, event: Event[RequestMessage | RejectMessage]):
    #     logging.info(f"{self}: new reply: {topic} -> {event}")
    #
    #     if event.payload.core_identifier not in self.pending_cores:
    #         logging.debug(f"{self}: {event.payload.core_identifier} sent a reply, but there is no related pending request")
    #         return
    #
    #     if isinstance(event.payload, RequestMessage):
    #         await self.request_event_handler(topic, event)
    #
    #     elif isinstance(event.payload, RejectMessage):
    #         await self.reject_event_handler(topic, event)
    #
    #     else:
    #         raise ValueError("Unexpected reply payload")




