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
from orbitalis.events.reply import RequestMessage, RejectMessage, RequestedOperation
from orbitalis.events.response import ResponseMessage
from orbitalis.core.need import Constraint, Need
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


    def build_offer_topic(self) -> str:
        return f"$handshake.{self.identifier}.offer.{uuid4()}"

    @classmethod
    def _is_plugin_operation_needed_by_need(cls, need: Constraint) -> bool:
        return need.minimum > 0 \
            or (need.mandatory is not None and len(need.mandatory) > 0) \
            or (need.maximum is None or need.maximum > 0)

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

    async def send_discover(self, needed_operations: Dict[str, Constraint]):

        offer_topic = self.build_offer_topic()

        if len(needed_operations) == 0:
            return

        await self.eventbus_client.subscribe(
            topic=offer_topic,
            handler=self.offer_event_handler
        )

        await self.eventbus_client.publish(
            self.discover_topic,
            DiscoverMessage(
                core_identifier=self.identifier,
                needed_operations=needed_operations,
                offer_topic=offer_topic
            ).into_event()
        )

    def is_plugin_operation_needed_and_pluggable(self, plugin_identifier: str, offered_operation: OfferedOperation) -> bool:

        not_satisfied_need = self.constraint_for_operation(offered_operation.operation_name)

        if not Core._is_plugin_operation_needed_by_need(not_satisfied_need):
            return False

        if not not_satisfied_need.is_compliance(plugin_identifier):
            return False

        if not not_satisfied_need.input_is_compatible(offered_operation.input):
            return False

        if not not_satisfied_need.output_is_compatible(offered_operation.output):
            return False

        return True

    def build_response_topic(self, plugin_identifier: str) -> str:
        return f"$handshake.{self.identifier}.{plugin_identifier}.response.{uuid4()}"

    def build_operation_output_topic(self, plugin_identifier: str, operation_name: str) -> str:
        return f"{operation_name}.{self.identifier}.{plugin_identifier}.output.{uuid4()}"

    @event_handler
    async def offer_event_handler(self, topic: str, event: Event[OfferMessage]):
        logging.debug(f"{self}: new offer: {topic} -> {event}")

        operations_to_request: List[OfferedOperation] = []
        operations_to_reject: List[OfferedOperation] = []
        for offered_operation in event.payload.offered_operations:
            if self.is_plugin_operation_needed_and_pluggable(event.payload.plugin_identifier, offered_operation):
                operations_to_request.append(offered_operation)
            else:
                operations_to_reject.append(offered_operation)


        response_topic: str = self.build_response_topic(event.payload.plugin_identifier)

        if len(operations_to_request) > 0:
            logging.debug(f"{self}: operations to request: {operations_to_request}")

            await self.eventbus_client.subscribe(
                topic=response_topic,
                handler=self.response_event_handler
            )

            requested_operations: Dict[str, RequestedOperation] = {}
            for offered_operation in operations_to_request:

                if not self.needed_operations[offered_operation.operation_name].constraint.output_is_compatible(offered_operation.output):
                    continue    # invalid offer

                output = offered_operation.output
                output_topic = self.build_operation_output_topic(event.payload.plugin_identifier, offered_operation.operation_name)

                incoming_close_connection_topic: str = self.build_incoming_close_connection_topic(
                    event.payload.plugin_identifier,
                    offered_operation.operation_name
                )

                self.add_pending_request(event.payload.plugin_identifier, offered_operation.operation_name, PendingRequest(
                    operation_name=offered_operation.operation_name,
                    remote_identifier=event.payload.plugin_identifier,
                    output_topic=output_topic,
                    output=output,
                    input=offered_operation.input,
                    incoming_close_connection_topic=incoming_close_connection_topic
                ))

                self.related_topics[event.payload.plugin_identifier].add(topic)
                self.related_topics[event.payload.plugin_identifier].add(event.payload.reply_topic)
                self.related_topics[event.payload.plugin_identifier].add(response_topic)

                requested_operations[offered_operation.operation_name] = RequestedOperation(
                    name=offered_operation.operation_name,
                    output_topic=output_topic,
                    setup_data=self.needed_operations[offered_operation.operation_name].setup_data,
                    core_close_connection_topic=incoming_close_connection_topic
                )

            try:
                await self.eventbus_client.publish(
                    topic=event.payload.reply_topic,
                    event=RequestMessage(
                        core_identifier=self.identifier,
                        response_topic=response_topic,
                        requested_operations=requested_operations
                    ).into_event()
                )

            except Exception as e:
                logging.error(f"{self}: {e}")

                for operation_name, result_topic in requested_operations.items():
                    self.remove_pending_request(event.payload.plugin_identifier, operation_name)

                if self.raise_exceptions:
                    raise e


        if len(operations_to_reject) > 0:
            logging.debug(f"{self}: operations to reject: {operations_to_reject}")

            await self.eventbus_client.publish(
                topic=event.payload.reply_topic,
                event=RejectMessage(
                    core_identifier=self.identifier,
                    description="Not needed anymore, sorry",
                    rejected_operations=[offered_operation.operation_name for offered_operation in operations_to_reject],
                ).into_event()
            )


    @event_handler
    async def response_event_handler(self, topic: str, event: Event[ResponseMessage]):
        # TODO: subscribe to keepalive topic and close topics

        logging.debug(f"{self}: new response: {topic} -> {event}")

        for borrowed_operation_name, confirmed_operation in event.payload.confirmed_operations.items():

            if event.payload.plugin_identifier not in self.pending_requests \
                or borrowed_operation_name not in self.pending_requests_by_remote_identifier(event.payload.plugin_identifier):
                logging.warning(f"{self}: operation {borrowed_operation_name} from plugin {event.payload.plugin_identifier} was not requested")
                continue

            try:

                await self.eventbus_client.subscribe(
                    self.pending_requests_by_remote_identifier(event.payload.plugin_identifier)[borrowed_operation_name].incoming_close_connection_topic,
                    self.close_connection_event_handler
                )

                if self.pending_requests_by_remote_identifier(event.payload.plugin_identifier)[borrowed_operation_name].output is not None:

                    handler: Optional[EventHandler] = None
                    if borrowed_operation_name in self.operation_sink:
                        handler = self.operation_sink[borrowed_operation_name]

                    if self.needed_operations[borrowed_operation_name].has_override_sink:
                        handler = self.needed_operations[borrowed_operation_name].override_sink

                    if handler is not None:
                        await self.eventbus_client.subscribe(
                            self.pending_requests_by_remote_identifier(event.payload.plugin_identifier)[borrowed_operation_name].output_topic,
                            handler
                        )

                self.pending_requests_by_remote_identifier(event.payload.plugin_identifier)[borrowed_operation_name].input_topic = confirmed_operation.input_topic
                self.pending_requests_by_remote_identifier(event.payload.plugin_identifier)[borrowed_operation_name].close_connection_to_remote_topic = confirmed_operation.plugin_close_connection_topic

                await self.promote_pending_request_to_connection(event.payload.plugin_identifier, borrowed_operation_name)

            except Exception as e:
                logging.error(f"{self}: {e}")

                await self.eventbus_client.unsubscribe(self.pending_requests_by_remote_identifier(event.payload.plugin_identifier)[borrowed_operation_name].output_topic)
                await self.eventbus_client.unsubscribe(self.pending_requests_by_remote_identifier(event.payload.plugin_identifier)[borrowed_operation_name].incoming_close_connection_topic)

                if self.raise_exceptions:
                    raise e

    @override
    async def _internal_start(self, *args, **kwargs):
        await super()._internal_start(*args, **kwargs)

        self.update_compliance()

        needed_operations: Dict[str, Constraint] = self.operation_to_discover()

        if len(needed_operations) > 0:
            await self.send_discover(needed_operations)

    @override
    async def _internal_stop(self, *args, **kwargs):
        await super()._internal_stop(*args, **kwargs)

        topics: List[str] = [
            self.discover_topic,
        ]

        await self.eventbus_client.multi_unsubscribe(topics, parallelize=self.parallelize)


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

