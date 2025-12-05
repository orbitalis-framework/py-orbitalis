import asyncio
import copy
import random
from datetime import datetime, timedelta
import logging
from dataclasses import dataclass, field
from typing import Type, override, Dict, Set, Optional, List
from busline.client.subscriber.event_handler import event_handler
from busline.client.subscriber.event_handler.event_handler import EventHandler
from busline.event.message.avro_message import AvroMessageMixin
from busline.event.event import Event

from orbitalis.core.sink import SinksProviderMixin
from orbitalis.core.state import CoreState
from orbitalis.events.discover import DiscoverMessage, DiscoverQuery
from orbitalis.events.offer import OfferMessage, OfferedOperation
from orbitalis.core.requirement import Constraint, OperationRequirement
from orbitalis.events.reply import RequestOperationMessage, RejectOperationMessage
from orbitalis.events.response import ConfirmConnectionMessage, OperationNoLongerAvailableMessage
from orbitalis.orbiter.connection import Connection
from orbitalis.orbiter.orbiter import Orbiter
from orbitalis.orbiter.pending_request import PendingRequest
from orbitalis.orbiter.schemaspec import Input
from orbitalis.state_machine.state_machine import StateMachine
from orbitalis.utils.task import fire_and_forget_task


@dataclass(kw_only=True)
class Core(SinksProviderMixin, StateMachine[CoreState], Orbiter):
    """
    Component which connects itself to plugins, in order to be able to execute their operations.

    Author: Nicola Ricciardi
    """

    discovering_interval: float = field(default=2)
    operation_requirements: Dict[str, OperationRequirement] = field(default_factory=dict)    # operation_name => OperationRequirement

    compliant_event: asyncio.Event = field(default_factory=asyncio.Event, init=False)
    not_compliant_event: asyncio.Event = field(default_factory=asyncio.Event, init=False)

    _last_discover_sent_at: Optional[datetime] = field(default=None)

    def __post_init__(self):
        super().__post_init__()

        self.state = CoreState.CREATED
        self.compliant_event.clear()
        self.not_compliant_event.set()

    @classmethod
    def _is_plugin_operation_required_by_constraint(cls, constraint: Constraint) -> bool:
        return constraint.minimum > 0 \
            or (constraint.mandatory is not None and len(constraint.mandatory) > 0) \
            or constraint.maximum is None \
            or (constraint.maximum is not None and constraint.maximum > 0)

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
                handler=self.__response_event_handler
            )

        # Offer subscription MUST be the second,
        # in order to have response event handler when offers will be able to be managed
        await self.eventbus_client.subscribe(
                topic=self.offer_topic,
                handler=self.__offer_event_handler
            )

        self.update_compliant()

        await self.send_discover_based_on_requirements()

    @override
    async def _internal_stop(self, *args, **kwargs):
        await super()._internal_stop(*args, **kwargs)

    @override
    async def _on_stopped(self, *args, **kwargs):
        await super()._on_stopped(*args, **kwargs)

        self.state = CoreState.STOPPED

    def _on_compliant(self):
        """
        Hook called when core becomes compliant
        """

    def _on_not_compliant(self):
        """
        Hook called when core becomes not compliant
        """

    @override
    async def _on_close_connection(self, connection: Connection):

        if connection.has_output:
            await self.eventbus_client.unsubscribe(connection.output_topic)

        self.update_compliant()

    def current_constraint_for_operation(self, operation_name: str) -> Constraint:
        """
        Return current constraint for operation based on current connections
        """

        if operation_name not in self.operation_requirements.keys():
            raise KeyError(f"operation {operation_name} is not required")

        constraint = copy.deepcopy(self.operation_requirements[operation_name]).constraint

        for connection in self.retrieve_connections(operation_name=operation_name):

            if constraint.mandatory is not None and connection.remote_identifier in constraint.mandatory:
                constraint.mandatory.remove(connection.remote_identifier)

            if constraint.maximum is not None:
                constraint.maximum = max(0, constraint.maximum - 1)

            constraint.minimum = max(0, constraint.minimum - 1)

        return constraint

    def is_compliant_for_operation(self, operation_name: str) -> bool:
        """
        Evaluate at run-time if core is compliant for given operation based on its configuration. It may be a time-consuming operation
        """

        constraint = self.current_constraint_for_operation(operation_name)

        if constraint.mandatory is not None and len(constraint.mandatory) > 0:
            return False

        if constraint.minimum > 0:
            return False

        return True

    def is_compliant(self) -> bool:
        """
        Evaluate at run-time if core is global compliant based on its configuration. It may be a very time-consuming operation
        """

        for operation_name in self.operation_requirements.keys():
            if not self.is_compliant_for_operation(operation_name):
                return False

        return True
    
    def switch_to_compliant(self):
        """
        Force core to compliant state
        """

        self.state = CoreState.COMPLIANT
        self._on_compliant()
        self.compliant_event.set()
        self.not_compliant_event.clear()

    def switch_to_not_compliant(self):
        """
        Force core to not compliant state
        """

        self.state = CoreState.NOT_COMPLIANT
        self._on_not_compliant()
        self.compliant_event.clear()
        self.not_compliant_event.set()

    def reset_compliance_events(self):
        self.compliant_event.clear()
        self.not_compliant_event.clear()

    def update_compliant(self):
        """
        Use `is_compliant` to update core's state
        """

        if self.state == CoreState.CREATED:

            if self.is_compliant():
                self.switch_to_compliant()
                return

            else:
                self.switch_to_not_compliant()
                return

        before_was_compliance = self.state == CoreState.COMPLIANT
        now_is_compliance = self.is_compliant()

        if before_was_compliance and not now_is_compliance:
            self.switch_to_not_compliant()

        elif not before_was_compliance and now_is_compliance:
            self.switch_to_compliant()

    def _operation_to_discover(self) -> Dict[str, Constraint]:
        """
        Return a dictionary `operation_name` => `constraint`, based on current connections. This operations should be discover
        """

        operation_requirements: Dict[str, Constraint] = {}
        for operation_name in self.operation_requirements.keys():
            constraint = self.current_constraint_for_operation(operation_name)

            discover_operation = Core._is_plugin_operation_required_by_constraint(constraint)

            if not discover_operation:
                logging.debug("%s: operation %s already satisfied", self, operation_name)
                continue

            operation_requirements[operation_name] = constraint

        return operation_requirements

    async def _on_send_discover(self, discover_message: DiscoverMessage):
        """
        Hook called before discover message is sent
        """

    async def send_discover_for_operations(self, operation_requirements: Dict[str, Constraint]):

        if len(operation_requirements) == 0:
            return

        discover_message = DiscoverMessage(
            core_identifier=self.identifier,
            queries=dict([(operation_name, DiscoverQuery.from_constraint(operation_name, constraint)) for operation_name, constraint in operation_requirements.items()]),
            offer_topic=self.offer_topic,
            core_keepalive_topic=self.keepalive_topic,
            core_keepalive_request_topic=self.keepalive_request_topic,
            considered_dead_after=self.consider_others_dead_after
        )

        await self._on_send_discover(discover_message)

        await self.eventbus_client.publish(
            self.discover_topic,
            discover_message
        )

        self._last_discover_sent_at = datetime.now()

    async def send_discover_based_on_requirements(self):
        operation_requirements: Dict[str, Constraint] = self._operation_to_discover()

        if len(operation_requirements) > 0:
            await self.send_discover_for_operations(operation_requirements)

    def _is_plugin_operation_required_and_pluggable(self, plugin_identifier: str, offered_operation: OfferedOperation) -> bool:

        not_satisfied_requirement = self.current_constraint_for_operation(offered_operation.name)

        if not Core._is_plugin_operation_required_by_constraint(not_satisfied_requirement):
            return False

        if not not_satisfied_requirement.is_compatible(plugin_identifier):
            return False

        if not not_satisfied_requirement.input_is_compatible(offered_operation.input):
            return False

        if not not_satisfied_requirement.output_is_compatible(offered_operation.output):
            return False

        return True

    def _build_operation_output_topic(self, plugin_identifier: str, operation_name: str) -> str:
        return f"{operation_name}.{self.identifier}.{plugin_identifier}.output"

    async def _get_setup_data(self, plugin_identifier: str, offered_operation: OfferedOperation) -> Optional[bytes]:
        """
        Hook called to obtain setup data which generally will be sent to plugins. By default, `default_setup_data` is used
        """

        return self.operation_requirements[offered_operation.name].default_setup_data

    async def __request_operation(self, plugin_identifier: str, reply_topic: str, offered_operation: OfferedOperation):

        logging.debug("%s: operations to request: %s", self, offered_operation)

        if not self.operation_requirements[offered_operation.name].constraint.output_is_compatible(offered_operation.output):
            return  # invalid offer

        output_topic: Optional[str] = None

        if offered_operation.output.has_output:
            output_topic = self._build_operation_output_topic(plugin_identifier, offered_operation.name)


        setup_data = await self._get_setup_data(plugin_identifier, offered_operation)

        incoming_close_connection_topic: str = self._build_incoming_close_connection_topic(
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
            )
        )


    async def __reject_operation(self, reply_topic: str, offered_operation: OfferedOperation):
        await self.eventbus_client.publish(
            reply_topic,
            RejectOperationMessage(
                core_identifier=self.identifier,
                operation_name=offered_operation.name
            )
        )

    async def _on_new_offer(self, offer_message: OfferMessage):
        """
        Hook called when a new offer arrives
        """

    @event_handler
    async def __offer_event_handler(self, topic: str, event: Event[OfferMessage]):
        logging.info("%s: new offer: %s -> %s", self, topic, event)

        await self._on_new_offer(event.payload)

        self.update_acquaintances(
            event.payload.plugin_identifier,
            keepalive_topic=event.payload.plugin_keepalive_topic,
            keepalive_request_topic=event.payload.plugin_keepalive_request_topic,
            consider_me_dead_after=event.payload.considered_dead_after
        )

        self.have_seen(event.payload.plugin_identifier)

        self._others_considers_me_dead_after[event.payload.plugin_identifier] = event.payload.considered_dead_after

        tasks = []
        for offered_operation in event.payload.offered_operations:
            if self._is_plugin_operation_required_and_pluggable(event.payload.plugin_identifier, offered_operation):
                tasks.append(self.__request_operation(
                    event.payload.plugin_identifier,
                    event.payload.reply_topic,
                    offered_operation
                ))
            else:
                tasks.append(self.__reject_operation(
                    event.payload.reply_topic,
                    offered_operation
                ))

        try:
            await asyncio.gather(*tasks)

        except Exception as e:
            logging.error("%s: %s", self, repr(e))

            if self.raise_exceptions:
                raise e

    async def _on_confirm_connection(self, confirm_connection_message: ConfirmConnectionMessage):
        """
        Hook called when a confirm connection arrives
        """

    async def __confirm_connection_event_handler(self, topic: str, event: Event[ConfirmConnectionMessage]):
        await self._on_confirm_connection(event.payload)

        plugin_identifier = event.payload.plugin_identifier
        operation_name = event.payload.operation_name

        if not self._is_pending(plugin_identifier, operation_name):
            logging.warning("%s: operation %s from plugin %s was not requested", self, operation_name, plugin_identifier)
            return

        pending_request = self._pending_requests_by_remote_identifier(plugin_identifier)[operation_name]

        async with pending_request.lock:
            if not self._is_pending(plugin_identifier, operation_name):
                logging.warning("%s: pending request (%s, %s) not available anymore", self, plugin_identifier, operation_name)
                return

            topics_to_unsubscribe_if_error: List[str] = []
            try:

                await self.eventbus_client.subscribe(
                    pending_request.incoming_close_connection_topic,
                    self._close_connection_event_handler
                )

                topics_to_unsubscribe_if_error.append(pending_request.incoming_close_connection_topic)

            except Exception as e:
                logging.error("%s: error during subscribing on close connection in response handling: %s", self, repr(e))

                if self.raise_exceptions:
                    raise e

            if pending_request.output_topic is not None:        # output is excepted
                handler: Optional[EventHandler] = None
                if operation_name in self.operation_sinks:
                    handler = self.operation_sinks[operation_name]

                if self.operation_requirements[operation_name].has_override_sink:
                    handler = self.operation_requirements[operation_name].override_sink

                if handler is not None:
                    try:
                        await self.eventbus_client.subscribe(
                            pending_request.output_topic,
                            handler
                        )

                        topics_to_unsubscribe_if_error.append(pending_request.output_topic)

                    except Exception as e:
                        logging.error("%s: error during subscribing to '%s' in response handling: %s", self, pending_request.output_topic, repr(e))

                        await self.eventbus_client.unsubscribe(pending_request.incoming_close_connection_topic)

                        if self.raise_exceptions:
                            raise e

            pending_request.input_topic = event.payload.operation_input_topic
            pending_request.close_connection_to_remote_topic = event.payload.plugin_side_close_operation_connection_topic

            try:
                self._promote_pending_request_to_connection(pending_request)

            except Exception as e:
                logging.error("%s: error during pending request promoting in response handling: %s", self, repr(e))

                await self.eventbus_client.multi_unsubscribe(topics_to_unsubscribe_if_error, parallelize=True)

                if self.raise_exceptions:
                    raise e

    async def _on_operation_no_longer_available(self, message: OperationNoLongerAvailableMessage):
        """
        Hook called when operation no longer available message arrives
        """

    async def __operation_no_longer_available_event_handler(self, topic: str, event: Event[OperationNoLongerAvailableMessage]):
        await self._on_operation_no_longer_available(event.payload)

        try:
            pending_request = self._pending_requests[event.payload.plugin_identifier][event.payload.operation_name]

            async with pending_request.lock:
                self._remove_pending_request(pending_request)

        except Exception as e:
            logging.warning("%s: pending request for operation %s of plugin %s can not be removed, maybe already removed", self, event.payload.operation_name, event.payload.plugin_identifier)

    async def _on_response(self):
        """
        Hook called when response message arrives
        """

    @event_handler
    async def __response_event_handler(self, topic: str, event: Event[ConfirmConnectionMessage | OperationNoLongerAvailableMessage]):

        logging.info("%s: new response: %s -> %s", self, topic, event)

        self.have_seen(event.payload.plugin_identifier)

        await self._on_response()

        if isinstance(event.payload, ConfirmConnectionMessage):
            await self.__confirm_connection_event_handler(topic, event)

        elif isinstance(event.payload, OperationNoLongerAvailableMessage):
            await self.__operation_no_longer_available_event_handler(topic, event)

        else:
            raise ValueError("Unexpected reply message")

        self.update_compliant()

    async def execute_distributed(self, operation_name: str, data: List[Optional[AvroMessageMixin]], fire_and_forget: bool = False) -> Set[str]:
        """
        Execute the operation by its name, distributing provided data among all compatible plugins.
        All data messages must be of the same type.

        Return the plugin identifiers the data has been sent to.
        """

        if len(data) == 0:
            return set()
        
        message_type: Type[AvroMessageMixin] | Type[None] = type(data[0])
        for index in range(1, len(data)):
            if not isinstance(data[index], message_type):
                raise ValueError("all data messages must be of the same type")

        connections = self.retrieve_connections(
            operation_name=operation_name,
            input=Input.empty() if message_type is type(None) else Input.from_message(message_type)
        )

        plugin_identifiers: Set[str] = set()
        tasks = []
        connection_index = 0

        for message in data:
            connection = connections[connection_index % len(connections)]
            connection_index += 1

            task = self.eventbus_client.publish(
                        connection.input_topic,
                        message
                    )

            if fire_and_forget:
                fire_and_forget_task(task)
            else:
                tasks.append(task)

            plugin_identifiers.add(connection.remote_identifier)

        if not fire_and_forget:
            await asyncio.gather(*tasks)

        return plugin_identifiers

    async def execute_sending_all(self, operation_name: str, data: Optional[AvroMessageMixin] = None, fire_and_forget: bool = False) -> Set[str]:
        """
        Execute the operation by its name, sending provided data to all compatible plugins.

        Return the plugin identifiers the data has been sent to.
        """

        connections = self.retrieve_connections(
            operation_name=operation_name,
            input=Input.empty() if data is None else Input.from_message(type(data))
        )

        plugin_identifiers: Set[str] = set()
        tasks = []
        for connection in connections:
            plugin_identifiers.add(connection.remote_identifier)

            task = self.eventbus_client.publish(
                    connection.input_topic,
                    data
                )
            
            if fire_and_forget:
                fire_and_forget_task(task)
            else:
                tasks.append(task)

        if not fire_and_forget:
            await asyncio.gather(*tasks)

        return plugin_identifiers
    
    async def execute_sending_any(self, operation_name: str, data: Optional[AvroMessageMixin] = None, fire_and_forget: bool = False) -> str:
        """
        Execute the operation by its name, sending provided data to only one random compatible plugin.

        Return the plugin identifier the data has been sent to.
        """

        connections = self.retrieve_connections(
            operation_name=operation_name,
            input=Input.empty() if data is None else Input.from_message(type(data))
        )

        connection = random.choice(connections)

        task = self.eventbus_client.publish(
            connection.input_topic,
            data
        )

        if fire_and_forget:
            fire_and_forget_task(task)
        else:
            await task

        return connection.remote_identifier

    async def execute_using_plugin(self, operation_name: str, plugin_identifier: str, data: Optional[AvroMessageMixin] = None, fire_and_forget: bool = False):
        """
        Execute the operation by its name, sending provided data to only one specific plugin.
        """

        connections = self.retrieve_connections(
            operation_name=operation_name,
            remote_identifier=plugin_identifier,
            input=Input.empty() if data is None else Input.from_message(type(data))
        )

        if len(connections) == 0:
            raise ValueError(f"no connection found for operation {operation_name} and plugin {plugin_identifier}")

        if len(connections) > 1:
            raise ValueError(f"multiple connections found for operation {operation_name} and plugin {plugin_identifier}")
        
        connection = connections[0]

        if connection.remote_identifier != plugin_identifier:
            raise ValueError(f"connection found for operation {operation_name} does not match plugin {plugin_identifier}")

        task = self.eventbus_client.publish(
            connection.input_topic,
            data
        )

        if fire_and_forget:
            fire_and_forget_task(task)
        else:
            await task

    async def execute(self, operation_name: str, data: Optional[AvroMessageMixin] | List[Optional[AvroMessageMixin]] = None, fire_and_forget: bool = False,
                      *, any: Optional[bool] = None, all: Optional[bool] = None, plugin_identifier: Optional[str] = None, distribute: Optional[bool] = None) -> Set[str]:
        """
        Execute the operation by its name, sending provided data.

        Return the plugin identifiers the data has been sent to.
        """

        if any is None and all is None and plugin_identifier is None and distribute is None:
            raise ValueError("mode (any/all/identifier/distribute) must be specified")

        messages: List[Optional[AvroMessageMixin]] = data if isinstance(data, list) else [data]

        if distribute is not None and distribute:
            return await self.execute_distributed(operation_name=operation_name, data=messages, fire_and_forget=fire_and_forget)

        if plugin_identifier is not None:
            for message in messages:
                sent = await self.execute_using_plugin(operation_name=operation_name, data=message, plugin_identifier=plugin_identifier, fire_and_forget=fire_and_forget)
                if not sent:
                    raise ValueError(f"can not send data to plugin {plugin_identifier} for operation {operation_name}")

            return {plugin_identifier}
        
        if any is not None and any:
            plugin_identifiers: Set[str] = set()
            for message in messages:
                plugin_id = await self.execute_sending_any(operation_name=operation_name, data=message, fire_and_forget=fire_and_forget)
                plugin_identifiers.add(plugin_id)

            return plugin_identifiers
        
        if all is not None and all:
            plugin_identifiers: Set[str] = set()
            for message in messages:
                sent_plugin_ids = await self.execute_sending_all(operation_name=operation_name, data=message, fire_and_forget=fire_and_forget)
                plugin_identifiers.update(sent_plugin_ids)

            return plugin_identifiers
        
        raise ValueError("invalid mode specified")

    async def sudo_execute(self, topic: str, data: Optional[AvroMessageMixin] = None):
        """
        Bypass all checks and send data to topic
        """

        await self.eventbus_client.publish(topic, data)

    @override
    async def _on_loop_start(self):
        await asyncio.sleep(self.discovering_interval)

    @override
    async def _on_loop_iteration(self):
        self.update_compliant()

        if self._last_discover_sent_at is None or (self._last_discover_sent_at + timedelta(seconds=self.discovering_interval)) < datetime.now():
            await self.send_discover_based_on_requirements()

    def __str__(self):
        return f"Core('{self.identifier}')"

