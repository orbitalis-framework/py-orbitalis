from __future__ import annotations

import logging
from typing import override, List, Optional, Any
from dataclasses import dataclass

from uuid import uuid4

from busline.client.subscriber.event_handler import event_handler
from busline.event.event import Event
from orbitalis.core.requirement import Constraint
from orbitalis.events.discover import DiscoverMessage, DiscoverQuery
from orbitalis.events.offer import OfferMessage, OfferedOperation
from orbitalis.events.reply import RejectOperationMessage, RequestOperationMessage
from orbitalis.events.response import ConfirmConnectionMessage, OperationNoLongerAvailableMessage
from orbitalis.orbiter.connection import Connection
from orbitalis.orbiter.orbiter import Orbiter
from orbitalis.orbiter.pending_request import PendingRequest
from orbitalis.plugin.operation import OperationsProviderMixin
from orbitalis.plugin.state import PluginState
from orbitalis.state_machine.state_machine import StateMachine


@dataclass(kw_only=True)
class Plugin(OperationsProviderMixin, StateMachine, Orbiter):
    """
    Component which provides a set of operations.

    Author: Nicola Ricciardi
    """

    def __post_init__(self):
        super().__post_init__()

        self.state = PluginState.CREATED

    @property
    def reply_topic(self) -> str:
        return f"$handshake.{self.identifier}.reply"

    @override
    async def _internal_start(self, *args, **kwargs):
        await super()._internal_start(*args, **kwargs)

        await self.eventbus_client.subscribe(
            self.reply_topic,
            self.__reply_event_handler
        )

        await self.eventbus_client.subscribe(
            self.discover_topic,
            self.__discover_event_handler
        )

        self.state = PluginState.RUNNING

    @override
    async def _internal_stop(self, *args, **kwargs):
        await super()._internal_stop(*args, **kwargs)

        topics: List[str] = [
            self.discover_topic,
            self.reply_topic
        ]

        await self.eventbus_client.multi_unsubscribe(topics, parallelize=True)

    @override
    async def _on_stopped(self, *args, **kwargs):
        await super()._on_stopped(*args, **kwargs)

        self.state = PluginState.STOPPED

    @override
    async def _on_close_connection(self, connection: Connection):

        if connection.has_input:
            await self.eventbus_client.unsubscribe(connection.input_topic)

    def __can_lend_to_core(self, core_identifier: str, operation_name: str) -> bool:
        if not self.operations[operation_name].policy.is_compatible(core_identifier):
            return False

        if self.operations[operation_name].policy.maximum is None or len(self.retrieve_connections(operation_name=operation_name)) < self.operations[operation_name].policy.maximum:
            return True

        return False

    def __allow_offer(self, core_identifier: str, core_needed_operation_name: str, core_discover_query: DiscoverQuery) -> bool:
        if core_needed_operation_name not in self.operations:
            return False

        # check compatibility with block/allow list
        if not core_discover_query.is_compatible(self.identifier):
            return False

        # check if already in pending request
        if core_identifier in self._pending_requests.keys() \
                and core_needed_operation_name in self._pending_requests_by_remote_identifier(core_identifier):
            return False

        # check if this plugin have already lent operation to core
        if len(self.retrieve_connections(remote_identifier=core_identifier,
                                         operation_name=core_needed_operation_name)) > 0:
            return False

        # check if there are slot available
        if self.operations[core_needed_operation_name].policy.maximum is not None:
            current_reserved_slot_for_operation: int = len(
                self.retrieve_connections(operation_name=core_needed_operation_name))

            for core_identifier, operations in self._pending_requests.items():
                if core_needed_operation_name in operations.keys():
                    current_reserved_slot_for_operation += 1

            if current_reserved_slot_for_operation >= self.operations[core_needed_operation_name].policy.maximum:
                return False

        # check input_schemas compatibility
        if not core_discover_query.input_is_compatible(self.operations[core_needed_operation_name].input):
            return False

        # check output_schemas compatibility
        if not core_discover_query.output_is_compatible(self.operations[core_needed_operation_name].output):
            return False

        if not self.__can_lend_to_core(core_identifier, core_needed_operation_name):
            return False

        return True

    async def _on_new_discover(self, discover_message: DiscoverMessage):
        """
        Hook called when a new discover message arrives
        """

    @event_handler
    async def __discover_event_handler(self, topic: str, event: Event[DiscoverMessage]):
        logging.info(f"{self}: new discover event from {event.payload.core_identifier}: {topic} -> {event}")

        await self._on_new_discover(event.payload)

        self.update_acquaintances(
            event.payload.core_identifier,
            keepalive_topic=event.payload.core_keepalive_topic,
            keepalive_request_topic=event.payload.core_keepalive_request_topic,
            consider_me_dead_after=event.payload.considered_dead_after
        )

        self.have_seen(event.payload.core_identifier)

        self._others_considers_me_dead_after[event.payload.core_identifier] = event.payload.considered_dead_after

        offerable_operations: List[str] = []

        for core_needed_operation_name, core_needed_operation_constraint in event.payload.queries.items():

            if self.__allow_offer(event.payload.core_identifier, core_needed_operation_name, core_needed_operation_constraint):
                offerable_operations.append(core_needed_operation_name)

        logging.debug(f"{self}: send offer for these operations: {offerable_operations}")
        if len(offerable_operations) > 0:
            await self.send_offer(
                event.payload.offer_topic,
                event.payload.core_identifier,
                offerable_operations
            )


    def _build_operation_input_topic_for_core(self, core_identifier: str, operation_name: str) -> str:
        return f"{operation_name}.{core_identifier}.{self.identifier}.input.{uuid4()}"

    async def _on_send_offer(self, offer_message: OfferMessage):
        """
        Hook called before offer is sent
        """

    async def send_offer(self, offer_topic: str, core_identifier: str, offerable_operations: List[str]):
        """
        Send a new offer message in given topic to given core identifier
        """

        if len(offerable_operations) == 0:
            return

        offered_operations: List[OfferedOperation] = []
        new_pending_requests: List[PendingRequest] = []

        for operation_name in offerable_operations:
            offered_operations.append(
                OfferedOperation(
                    name=operation_name,
                    input=self.operations[operation_name].input,
                    output=self.operations[operation_name].output
                )
            )

            pending_request = PendingRequest(
                operation_name=operation_name,
                remote_identifier=core_identifier,
                input=self.operations[operation_name].input,
                output=self.operations[operation_name].output
            )

            self._add_pending_request(pending_request)
            new_pending_requests.append(pending_request)

        try:
            offer_message = OfferMessage(
                    plugin_identifier=self.identifier,
                    offered_operations=offered_operations,
                    reply_topic=self.reply_topic,
                    plugin_keepalive_topic=self.keepalive_topic,
                    plugin_keepalive_request_topic=self.keepalive_request_topic,
                    considered_dead_after=self.consider_others_dead_after
                )

            await self._on_send_offer(offer_message)

            await self.eventbus_client.publish(offer_topic, offer_message)

        except Exception as e:
            logging.error(f"{self}: {repr(e)}")

            for pending_request in new_pending_requests:
                try:
                    async with pending_request.lock:
                        self._remove_pending_request(pending_request)
                except Exception:
                    pass

            if self.raise_exceptions:
                raise e

    async def _on_reject(self, message: RejectOperationMessage):
        """
        Hook called when a reject message arrives
        """

    async def __reject_event_handler(self, topic: str, event: Event[RejectOperationMessage]):
        logging.debug(f"{self}: core {event.payload.core_identifier} rejects plug request for this operation: {event.payload.operation_name}")

        await self._on_reject(event.payload)

        try:
            pending_request = self._pending_requests[event.payload.core_identifier][event.payload.operation_name]

            async with pending_request.lock:
                self._remove_pending_request(pending_request)

        except Exception as e:
            logging.warning(f"{self}: pending request ('{event.payload.core_identifier}', '{event.payload.operation_name}') can not be removed")

    async def _setup_operation(self, core_identifier: str, operation_name: str, setup_data: Optional[bytes]):
        """
        Hook called to set up operation when connection is created
        """

    async def _plug_operation_into_core(self, core_identifier: str, response_topic: str, operation_name: str, setup_data: Optional[bytes]):
        """

        Return (operation_input_topic, plugin_side_close_operation_connection_topic)
        """

        topics_to_unsubscribe_if_error: List[str] = []

        operation_input_topic: str = self._build_operation_input_topic_for_core(core_identifier, operation_name)

        plugin_side_close_operation_connection_topic = self._build_incoming_close_connection_topic(
            core_identifier,
            operation_name
        )

        try:
            await self.eventbus_client.subscribe(operation_input_topic, self.operations[operation_name].handler)
            topics_to_unsubscribe_if_error.append(operation_input_topic)

            await self.eventbus_client.subscribe(
                plugin_side_close_operation_connection_topic,
                self._close_connection_event_handler
            )
            topics_to_unsubscribe_if_error.append(plugin_side_close_operation_connection_topic)

            if setup_data is not None:
                await self._setup_operation(
                    core_identifier,
                    operation_name,
                    setup_data
                )

            await self.eventbus_client.publish(
                response_topic,
                ConfirmConnectionMessage(
                    plugin_identifier=self.identifier,
                    operation_name=operation_name,
                    operation_input_topic=operation_input_topic,
                    plugin_side_close_operation_connection_topic=plugin_side_close_operation_connection_topic
                )
            )

            return operation_input_topic, plugin_side_close_operation_connection_topic

        except Exception as e:
            logging.error(f"{self}: error during plug operation '{operation_name}' into core '{core_identifier}': {repr(e)}")

            await self.eventbus_client.multi_unsubscribe(topics_to_unsubscribe_if_error, parallelize=True)

            raise e

    async def _on_request(self, message: RequestOperationMessage):
        """
        Hook called when a new request message arrives
        """

    async def __request_operation_event_handler(self, topic: str, event: Event[RequestOperationMessage]):

        await self._on_request(event.payload)

        core_identifier = event.payload.core_identifier
        operation_name = event.payload.operation_name

        logging.debug(f"{self}: core {core_identifier} confirms plug request for this operation: {operation_name}")

        if not self._is_pending(core_identifier, operation_name):
            logging.warning(f"{self}: pending request for ('{core_identifier}', '{operation_name}') not found")
            return

        pending_request = self._pending_requests_by_remote_identifier(core_identifier)[operation_name]

        async with pending_request.lock:
            if not self._is_pending(core_identifier, operation_name):
                logging.warning(f"{self}: pending request ({core_identifier}, {operation_name}) not available anymore")
                return

            try:
                if not self.__can_lend_to_core(core_identifier, operation_name):
                    logging.debug(f"{self}: can not lend to core '{core_identifier}' operation: {operation_name}")

                    await self.eventbus_client.publish(
                        event.payload.response_topic,
                        OperationNoLongerAvailableMessage(
                            plugin_identifier=self.identifier,
                            operation_name=operation_name
                        )
                    )

                    self._remove_pending_request(pending_request)

                else:

                    operation_input_topic, plugin_side_close_operation_connection_topic = await self._plug_operation_into_core(
                        core_identifier,
                        event.payload.response_topic,
                        operation_name,
                        event.payload.setup_data
                    )

                    pending_request.incoming_close_connection_topic = plugin_side_close_operation_connection_topic
                    pending_request.input_topic = operation_input_topic
                    pending_request.output_topic = event.payload.output_topic
                    pending_request.close_connection_to_remote_topic = event.payload.core_side_close_operation_connection_topic

                    self._promote_pending_request_to_connection(pending_request)

            except Exception as e:
                logging.error(f"{self}: error during confirm pending request': {repr(e)}")

                if self.raise_exceptions:
                    raise e


    async def _on_reply(self):
        """
        Hook called when a new reply message arrives
        """

    @event_handler
    async def __reply_event_handler(self, topic: str, event: Event[RequestOperationMessage | RejectOperationMessage]):
        logging.info(f"{self}: new reply: {topic} -> {event}")

        self.have_seen(event.payload.core_identifier)

        await self._on_reply()

        if isinstance(event.payload, RequestOperationMessage):
            await self.__request_operation_event_handler(topic, event)

        elif isinstance(event.payload, RejectOperationMessage):
            await self.__reject_event_handler(topic, event)

        else:
            raise ValueError("Unexpected reply message")

    def __str__(self):
        return f"Plugin('{self.identifier}')"


