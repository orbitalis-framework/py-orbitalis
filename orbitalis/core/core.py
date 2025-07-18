import asyncio
import copy
import random
from abc import abstractmethod, ABC
from collections import defaultdict
from datetime import datetime
import logging
from dataclasses import dataclass, field
from typing import override, Dict, Set, Optional, List, Tuple
from uuid import uuid4
from busline.client.subscriber.topic_subscriber.event_handler import schemafull_event_handler, event_handler
from busline.client.subscriber.topic_subscriber.event_handler.event_handler import EventHandler
from busline.event.avro_payload import AvroEventPayload
from busline.event.event import Event
from orbitalis.core.sink import SinksProviderMixin
from orbitalis.core.state import CoreState
from orbitalis.events.close_connection import GracefulCloneConnectionMessage, GracelessCloneConnectionMessage
from orbitalis.events.discover import DiscoverMessage
from orbitalis.events.offer import OfferMessage, OfferedOperation
from orbitalis.core.need import Constraint, Need
from orbitalis.events.reply import RequestOperationMessage, RejectOperationMessage
from orbitalis.events.response import ConfirmConnectionMessage, OperationNoLongerAvailableMessage
from orbitalis.orbiter.connection import Connection
from orbitalis.orbiter.orbiter import Orbiter
from orbitalis.orbiter.pending_request import PendingRequest
from orbitalis.state_machine.state_machine import StateMachine


@dataclass(kw_only=True)
class Core(Orbiter, SinksProviderMixin, StateMachine[CoreState]):

    discovering_interval: float = field(default=2)
    needed_operations: Dict[str, Need] = field(default_factory=dict)    # operation_name => Need

    def __post_init__(self):
        super().__post_init__()

        self.state = CoreState.CREATED

    @classmethod
    def _is_plugin_operation_needed_by_need(cls, need: Constraint) -> bool:
        return need.minimum > 0 \
            or (need.mandatory is not None and len(need.mandatory) > 0) \
            or (need.maximum is None or need.maximum > 0)

    @property
    def offer_topic(self) -> str:
        return f"$handshake.{self.identifier}.offer"

    @property
    def response_topic(self) -> str:
        return f"$handshake.{self.identifier}.response"


    @override
    async def _internal_start(self, *args, **kwargs):
        await super()._internal_start(*args, **kwargs)

        await self.eventbus_client.subscribe(
                topic=self.response_topic,
                handler=self._response_event_handler
            )

        await self.eventbus_client.subscribe(
                topic=self.offer_topic,
                handler=self._offer_event_handler
            )

        self.update_compliance()

        await self.send_discover_based_on_needs()

    @override
    async def _internal_stop(self, *args, **kwargs):
        await super()._internal_stop(*args, **kwargs)

    def _on_compliance(self):
        """
        Hook

        TODO: doc
        """

    def _on_not_compliance(self):
        """
        Hook

        TODO: doc
        """

    @override
    async def _on_close_connection(self, connection: Connection):

        topics: List[str] = [
            connection.incoming_close_connection_topic,
        ]

        if connection.output_topic is not None and connection.output.has_output:
            topics.append(connection.output_topic)

        await self.eventbus_client.multi_unsubscribe(topics, parallelize=True)

    def constraint_for_operation(self, operation_name: str) -> Constraint:

        if operation_name not in self.needed_operations.keys():
            raise KeyError(f"operation {operation_name} is not required")

        constraint = copy.deepcopy(self.needed_operations[operation_name]).constraint

        for connection in self.retrieve_connections(operation_name=operation_name):

            if constraint.mandatory is not None and connection.remote_identifier in constraint.mandatory:
                constraint.mandatory.remove(connection.remote_identifier)

            if constraint.maximum is not None:
                constraint.maximum = max(0, constraint.maximum - 1)

            constraint.minimum = max(0, constraint.minimum - 1)

        return constraint

    def is_compliance_for_operation(self, operation_name: str) -> bool:
        need = self.constraint_for_operation(operation_name)

        if need.mandatory is not None and len(need.mandatory) > 0:
            return False

        if need.minimum > 0:
            return False

        return True

    def is_compliance(self) -> bool:

        for operation_name in self.needed_operations.keys():
            if not self.is_compliance_for_operation(operation_name):
                return False

        return True

    def update_compliance(self):
        # TODO: evitare di chiamare sempre is_compliance e gli hook

        if self.is_compliance():
            self.state = CoreState.COMPLIANT
            # TODO: self._on_compliance()
        else:
            self.state = CoreState.NOT_COMPLIANT
            # TODO: self._on_not_compliance()


    def operation_to_discover(self) -> Dict[str, Constraint]:
        needed_operations: Dict[str, Constraint] = {}
        for operation_name in self.needed_operations.keys():
            not_satisfied_need = self.constraint_for_operation(operation_name)

            discover_operation = Core._is_plugin_operation_needed_by_need(not_satisfied_need)

            if not discover_operation:
                logging.debug(f"{self}: operation {operation_name} already satisfied")
                continue

            needed_operations[operation_name] = not_satisfied_need

        return needed_operations

    async def send_discover_for_operations(self, needed_operations: Dict[str, Constraint]):

        if len(needed_operations) == 0:
            return

        await self.eventbus_client.subscribe(
            topic=self.offer_topic,
            handler=self._offer_event_handler
        )

        await self.eventbus_client.publish(
            self.discover_topic,
            DiscoverMessage(
                core_identifier=self.identifier,
                needed_operations=needed_operations,
                offer_topic=self.offer_topic,
                core_keepalive_topic=self.keepalive_topic,
                core_keepalive_request_topic=self.keepalive_request_topic
            ).into_event()
        )

    async def send_discover_based_on_needs(self):
        needed_operations: Dict[str, Constraint] = self.operation_to_discover()

        if len(needed_operations) > 0:
            await self.send_discover_for_operations(needed_operations)

    def is_plugin_operation_needed_and_pluggable(self, plugin_identifier: str, offered_operation: OfferedOperation) -> bool:

        not_satisfied_need = self.constraint_for_operation(offered_operation.name)

        if not Core._is_plugin_operation_needed_by_need(not_satisfied_need):
            return False

        if not not_satisfied_need.is_compliance(plugin_identifier):
            return False

        if not not_satisfied_need.input_is_compatible(offered_operation.input):
            return False

        if not not_satisfied_need.output_is_compatible(offered_operation.output):
            return False

        return True

    def build_operation_output_topic(self, plugin_identifier: str, operation_name: str) -> str:
        return f"{operation_name}.{self.identifier}.{plugin_identifier}.output"

    async def _get_setup_data(self, plugin_identifier: str, offered_operation: OfferedOperation) -> Optional[str]:
        """
        TODO: doc
        """

        return self.needed_operations[offered_operation.name].default_setup_data

    async def _request_operation(self, plugin_identifier: str, reply_topic: str, offered_operation: OfferedOperation):

        logging.debug(f"{self}: operations to request: {offered_operation}")

        if not self.needed_operations[offered_operation.name].constraint.output_is_compatible(offered_operation.output):
            return  # invalid offer

        output_topic: Optional[str] = None

        if offered_operation.output.has_output:
            output_topic = self.build_operation_output_topic(plugin_identifier, offered_operation.name)


        setup_data = await self._get_setup_data(plugin_identifier, offered_operation)

        incoming_close_connection_topic: str = self.build_incoming_close_connection_topic(
            plugin_identifier,
            offered_operation.name
        )

        self._add_pending_request(PendingRequest(
            operation_name=offered_operation.name,
            remote_identifier=plugin_identifier,
            output_topic=output_topic,
            output=offered_operation.output,
            input=offered_operation.input,
            incoming_close_connection_topic=incoming_close_connection_topic
        ))

        await self.eventbus_client.publish(
            reply_topic,
            RequestOperationMessage(
                core_identifier=self.identifier,
                operation_name=offered_operation.name,
                response_topic=self.response_topic,
                output_topic=output_topic,
                core_side_close_operation_connection_topic=incoming_close_connection_topic,
                setup_data=setup_data
            ).into_event()
        )


    async def _reject_operation(self, plugin_identifier: str, reply_topic: str, offered_operation: OfferedOperation):
        await self.eventbus_client.publish(
            reply_topic,
            RejectOperationMessage(
                core_identifier=self.identifier,
                operation_name=offered_operation.name
            ).into_event()
        )

    @event_handler
    async def _offer_event_handler(self, topic: str, event: Event[OfferMessage]):
        logging.info(f"{self}: new offer: {topic} -> {event}")

        self.update_acquaintances(
            event.payload.plugin_identifier,
            keepalive_topic=event.payload.plugin_keepalive_topic,
            keepalive_request_topic=event.payload.plugin_keepalive_request_topic
        )

        self.have_seen(event.payload.plugin_identifier)

        tasks = []
        for offered_operation in event.payload.offered_operations:
            if self.is_plugin_operation_needed_and_pluggable(event.payload.plugin_identifier, offered_operation):
                tasks.append(asyncio.create_task(self._request_operation(
                    event.payload.plugin_identifier,
                    event.payload.reply_topic,
                    offered_operation
                )))
            else:
                tasks.append(asyncio.create_task(self._reject_operation(
                    event.payload.plugin_identifier,
                    event.payload.reply_topic,
                    offered_operation
                )))

        try:
            await asyncio.gather(*tasks)

        except Exception as e:
            logging.error(f"{self}: {repr(e)}")

            if self.raise_exceptions:
                raise e


    async def _confirm_connection_event_handler(self, topic: str, event: Event[ConfirmConnectionMessage]):
        plugin_identifier = event.payload.plugin_identifier
        operation_name = event.payload.operation_name

        if not self.is_pending(plugin_identifier, operation_name):
            logging.warning(f"{self}: operation {operation_name} from plugin {plugin_identifier} was not requested")
            return

        pending_request = self.pending_requests_by_remote_identifier(plugin_identifier)[operation_name]

        async with pending_request.lock:
            if not self.is_pending(plugin_identifier, operation_name):
                logging.warning(f"{self}: pending request ({plugin_identifier}, {operation_name}) not available anymore")
                return

            topics_to_unsubscribe_if_error: List[str] = []
            try:

                await self.eventbus_client.subscribe(
                    pending_request.incoming_close_connection_topic,
                    self._close_connection_event_handler
                )

                topics_to_unsubscribe_if_error.append(pending_request.incoming_close_connection_topic)

            except Exception as e:
                logging.error(f"{self}: error during subscribing on close connection in response handling: {repr(e)}")

                if self.raise_exceptions:
                    raise e

            if pending_request.output_topic is not None:        # output is excepted
                handler: Optional[EventHandler] = None
                if operation_name in self.operation_sink:
                    handler = self.operation_sink[operation_name]

                if self.needed_operations[operation_name].has_override_sink:
                    handler = self.needed_operations[operation_name].override_sink

                if handler is not None:
                    try:
                        await self.eventbus_client.subscribe(
                            pending_request.output_topic,
                            handler
                        )

                        topics_to_unsubscribe_if_error.append(pending_request.output_topic)

                    except Exception as e:
                        logging.error(
                            f"{self}: error during subscribing to '{pending_request.output_topic}' in response handling: {repr(e)}")

                        await self.eventbus_client.unsubscribe(pending_request.incoming_close_connection_topic)

                        if self.raise_exceptions:
                            raise e

            pending_request.input_topic = event.payload.operation_input_topic
            pending_request.close_connection_to_remote_topic = event.payload.plugin_side_close_operation_connection_topic

            try:
                await self.promote_pending_request_to_connection(pending_request)

            except Exception as e:
                logging.error(f"{self}: error during pending request promoting in response handling: {repr(e)}")

                await self.eventbus_client.multi_unsubscribe(topics_to_unsubscribe_if_error, parallelize=True)

                if self.raise_exceptions:
                    raise e

    async def _operation_no_longer_available_event_handler(self, topic: str, event: Event[OperationNoLongerAvailableMessage]):
        self._remove_pending_request(
            event.payload.plugin_identifier,
            event.payload.operation_name
        )

    @event_handler
    async def _response_event_handler(self, topic: str, event: Event[ConfirmConnectionMessage | OperationNoLongerAvailableMessage]):

        logging.info(f"{self}: new response: {topic} -> {event}")

        if isinstance(event.payload, ConfirmConnectionMessage):
            await self._confirm_connection_event_handler(topic, event)

        elif isinstance(event.payload, OperationNoLongerAvailableMessage):
            await self._operation_no_longer_available_event_handler(topic, event)

        else:
            raise ValueError("Unexpected reply payload")


    def __check_compatibility_connection_payload(self, connection: Connection, payload: Optional[AvroEventPayload], undefined_is_compatible: bool) -> bool:
        if not connection.input.has_input:
            return False

        if payload is None:
            if connection.input.support_empty_schema:
                return True
            else:
                return False

        if connection.input.is_compatible_with_schema(payload.avro_schema(), undefined_is_compatible=undefined_is_compatible):
            return True

        return False

    async def execute(self, operation_name: str, payload: Optional[AvroEventPayload] = None,
        /, any: bool = False, all: bool = False, plugin_identifier: Optional[str] = None,
            undefined_is_compatible: Optional[bool] = None):
        """
        Execute the operation by its name.

        You must specify which plugin must be used, otherwise ValueError is raised.
        """

        if int(any) + int(all) + int(plugin_identifier is not None) > 1:
            raise ValueError("You must chose only one between 'any', 'all' or 'plugin_identifier'")

        if undefined_is_compatible is None:
            undefined_is_compatible = self.undefined_is_compatible

        topics: Set[str] = set()

        connections = self.retrieve_connections(operation_name=operation_name)

        if plugin_identifier is not None:

            for connection in connections:
                if connection.remote_identifier != plugin_identifier:
                    continue

                if self.__check_compatibility_connection_payload(connection, payload, undefined_is_compatible):
                    topics.add(connection.input_topic)

        elif all or any:
            for connection in connections:
                if self.__check_compatibility_connection_payload(connection, payload, undefined_is_compatible):
                    topics.add(connection.input_topic)

            if any:
                topics = { random.choice(list(topics)) }

        else:
            raise ValueError("mode (any/all/identifier) must be specified")


        await self.eventbus_client.multi_publish(list(topics), payload.into_event() if payload is not None else Event())


    async def sudo_execute(self, topic: str, payload: Optional[AvroEventPayload] = None):
        await self.eventbus_client.publish(topic, payload.into_event() if payload is not None else None)


    def __str__(self):
        return f"Core('{self.identifier}')"

