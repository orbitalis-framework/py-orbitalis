from __future__ import annotations

import logging
from collections import defaultdict
from typing import override, Dict, List, Tuple, Optional
from abc import ABC
from dataclasses import dataclass, field

from datetime import datetime
from busline.client.subscriber.topic_subscriber.event_handler import schemafull_event_handler, event_handler
from busline.event.event import Event
from orbitalis.core.need import Constraint, SetupData
from orbitalis.events.discover import DiscoverMessage
from orbitalis.events.offer import OfferMessage, OfferedOperation
from orbitalis.events.reply import RequestMessage, RejectMessage
from orbitalis.events.response import ResponseMessage
from orbitalis.events.wellknown_topic import WellKnownTopic
from orbitalis.orbiter.orbiter import Orbiter
from orbitalis.orbiter.pending_request import PendingRequest
from orbitalis.plugin.operation import Operation, OperationsProviderMixin
from orbitalis.plugin.state import PluginState
from orbitalis.state_machine.state_machine import StateMachine


@dataclass(kw_only=True)
class Plugin(Orbiter, StateMachine, OperationsProviderMixin, ABC):
    """

    TODO: a way to provide dynamic policies for operations

    Author: Nicola Ricciardi
    """

    def __post_init__(self):
        super().__post_init__()

        self.state = PluginState.CREATED

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

    def __allow_offer(self, core_identifier: str, core_needed_operation_name: str, core_needed_operation_constraint: Constraint) -> bool:
        if core_needed_operation_name not in self.operations:
            return False

        # check compatibility with block/allow list
        if (core_needed_operation_constraint.blocklist is not None and self.identifier in core_needed_operation_constraint.blocklist) \
                or (core_needed_operation_constraint.allowlist is not None and self.identifier not in core_needed_operation_constraint.allowlist):
            return False

        # check if already in pending request
        if core_identifier in self.pending_requests.keys() \
                and core_needed_operation_name in self.pending_requests_by_remote_identifier(core_identifier):
            return False

        # check if this plugin have already lent operation to core
        if len(self.retrieve_connections(remote_identifier=core_identifier,
                                         operation_name=core_needed_operation_name)) > 0:
            return False

        # check if there are slot available
        if self.operations[core_needed_operation_name].policy.maximum is not None:
            current_reserved_slot_for_operation: int = len(
                self.retrieve_connections(operation_name=core_needed_operation_name))

            for core_identifier, operations in self.pending_requests.items():
                if core_needed_operation_name in operations.keys():
                    current_reserved_slot_for_operation += 1

            if current_reserved_slot_for_operation >= self.operations[core_needed_operation_name].policy.maximum:
                return False

        # check input_schemas compatibility
        if not core_needed_operation_constraint.input_is_compatible(self.operations[core_needed_operation_name].input):
            return False

        # check output_schemas compatibility
        if not core_needed_operation_constraint.output_is_compatible(self.operations[core_needed_operation_name].output):
            return False

        if not self.can_lend_to_core(core_identifier, core_needed_operation_name):
            return False

        return True

    @event_handler
    async def discover_event_handler(self, topic: str, event: Event[DiscoverMessage]):
        logging.info(f"{self}: new discover event from {event.payload.core_identifier}: {topic} -> {event}")

        offerable_operations: List[str] = []

        for core_needed_operation_name, core_needed_operation_constraint in event.payload.needed_operations.items():

            if self.__allow_offer(event.payload.core_identifier, core_needed_operation_name, core_needed_operation_constraint):
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
                    input=self.operations[operation_name].input,
                    output=self.operations[operation_name].output
                )
            )

        reply_topic = self.build_reply_topic(core_identifier)

        for operation_name in offerable_operations:
            self.add_pending_request(core_identifier, operation_name, PendingRequest(
                operation_name=operation_name,
                remote_identifier=core_identifier,
                offer_topic=offer_topic,
                reply_topic=reply_topic,
                input=self.operations[operation_name].input,
                output=self.operations[operation_name].output
            ))

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
                self.remove_pending_request(core_identifier, operation_name)

            if self.raise_exceptions:
                raise e

    async def reject_event_handler(self, topic: str, event: Event[RejectMessage]):
        logging.debug(f"{self}: core {event.payload.core_identifier} rejects plug request for these operations: {event.payload.rejected_operations}")

        for operation_name in event.payload.rejected_operations:
            self.pending_requests_by_remote_identifier(event.payload.core_identifier).pop(operation_name, None)

    async def _setup_operation(self, core_identifier: str, operation_name: str, setup_data: SetupData):
        """
        Hook

        TODO: doc
        """

    async def request_event_handler(self, topic: str, event: Event[RequestMessage]):

        # TODO: subscribe to keepalive topic and close topics

        logging.debug(f"{self}: core {event.payload.core_identifier} confirms plug request for these operations: {event.payload.requested_operations}")

        confirmed_operations: List[str] = []
        operations_no_longer_available: List[str] = []
        for requested_operation in event.payload.requested_operations.values():
            plug_confirmed: bool = self.can_lend_to_core(event.payload.core_identifier, requested_operation.name)

            logging.debug(f"{self}: can lend to {event.payload.core_identifier}? {plug_confirmed}")

            if plug_confirmed:
                confirmed_operations.append(requested_operation.name)
            else:
                operations_no_longer_available.append(requested_operation.name)

        subscribed_topics: List[str] = []
        confirmed_operations_and_input_topic: Dict[str, str] = {}
        for confirmed_operation_name in confirmed_operations:
            input_topic: str = self.build_operation_input_topic_for_core(event.payload.core_identifier, confirmed_operation_name)
            output_topic: str = event.payload.requested_operations[confirmed_operation_name].output_topic

            try:
                await self.eventbus_client.subscribe(input_topic, self.operations[confirmed_operation_name].handler)
                subscribed_topics.append(input_topic)

                self.pending_requests_by_remote_identifier(event.payload.core_identifier)[confirmed_operation_name].input_topic = input_topic
                self.pending_requests_by_remote_identifier(event.payload.core_identifier)[confirmed_operation_name].output_topic = output_topic

                confirmed_operations_and_input_topic[confirmed_operation_name] = input_topic

            except Exception as e:
                logging.error(f"{self}: {e}")

                await self.eventbus_client.multi_unsubscribe(subscribed_topics)

                if self.raise_exceptions:
                    raise e

                return

        try:

            await self.eventbus_client.publish(
                event.payload.response_topic,
                ResponseMessage(
                    plugin_identifier=self.identifier,
                    confirmed_operations=confirmed_operations_and_input_topic,
                    operations_no_longer_available=operations_no_longer_available
                ).into_event()
            )

            for operation_name in operations_no_longer_available:
                self.remove_pending_request(event.payload.core_identifier, operation_name)

            for operation_name in confirmed_operations:
                self.promote_pending_request_to_connection(event.payload.core_identifier, operation_name)

                setup_data = event.payload.requested_operations[operation_name].setup_data

                if setup_data is not None:
                    await self._setup_operation(
                        event.payload.core_identifier,
                        operation_name,
                        setup_data
                    )


        except Exception as e:
            logging.error(f"{self}: {e}")

            await self.eventbus_client.multi_unsubscribe(
                [input_topic for input_topic in confirmed_operations_and_input_topic.values()]
            )

            if self.raise_exceptions:
                raise e

    @event_handler
    async def reply_event_handler(self, topic: str, event: Event[RequestMessage | RejectMessage]):
        logging.info(f"{self}: new reply: {topic} -> {event}")

        if event.payload.core_identifier not in self.pending_requests:
            logging.debug(f"{self}: {event.payload.core_identifier} sent a reply, but there is no related pending request")
            return

        if isinstance(event.payload, RequestMessage):
            await self.request_event_handler(topic, event)

        elif isinstance(event.payload, RejectMessage):
            await self.reject_event_handler(topic, event)

        else:
            raise ValueError("Unexpected reply payload")


    def __str__(self):
        return f"Plugin('{self.identifier}')"


