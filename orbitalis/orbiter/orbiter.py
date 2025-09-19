import asyncio
import logging
from abc import ABC
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set
import uuid

from busline.client.pubsub_client import PubSubClient
from busline.client.subscriber.event_handler import event_handler
from busline.event.event import Event
from orbitalis.events.close_connection import GracefulCloseConnectionMessage, GracelessCloneConnectionMessage, \
    CloseConnectionAckMessage
from orbitalis.events.keepalive import KeepaliveRequestMessage, KeepaliveMessage
from orbitalis.orbiter.connection import Connection
from orbitalis.orbiter.pending_request import PendingRequest
from orbitalis.orbiter.schemaspec import Output, Input
from orbitalis.plugin.operation import Operation

DEFAULT_DISCOVER_TOPIC = "$handshake.discover"
DEFAULT_LOOP_INTERVAL = 1
DEFAULT_PENDING_REQUESTS_EXPIRE_AFTER = 60.0
DEFAULT_SEND_KEEPALIVE_BEFORE_TIMELIMIT = 10.0
DEFAULT_CONSIDERED_DEAD_AFTER = 120.0
DEFAULT_GRACEFUL_CLOSE_TIMEOUT = 300.0


@dataclass(kw_only=True)
class Orbiter(ABC):
    """
    Base class which provides common capabilities to components.
    It manages pending requests, connections, keepalive and connection close procedure.
    In addiction, it has useful shared methods and main loop.

    Author: Nicola Ricciardi
    """

    eventbus_client: PubSubClient

    identifier: str = field(default_factory=lambda: str(uuid.uuid4()))

    discover_topic: str = field(default=DEFAULT_DISCOVER_TOPIC)
    raise_exceptions: bool = field(default=False)

    loop_interval: float = field(default=DEFAULT_LOOP_INTERVAL)

    close_connection_if_unused_after: Optional[float] = field(default=None)
    pending_requests_expire_after: Optional[float] = field(default=DEFAULT_PENDING_REQUESTS_EXPIRE_AFTER)
    consider_others_dead_after: Optional[float] = field(default=DEFAULT_CONSIDERED_DEAD_AFTER)
    send_keepalive_before_timelimit: float = field(default=DEFAULT_SEND_KEEPALIVE_BEFORE_TIMELIMIT)
    graceful_close_timeout: Optional[float] = field(default=DEFAULT_GRACEFUL_CLOSE_TIMEOUT)

    with_loop: bool = field(default=True)

    _others_considers_me_dead_after: Dict[str, float] = field(default_factory=dict, init=False)     # remote_identifier => time
    _remote_keepalive_request_topics: Dict[str, str] = field(default_factory=dict, init=False)   # remote_identifier => keepalive_request_topic
    _remote_keepalive_topics: Dict[str, str] = field(default_factory=dict, init=False)   # remote_identifier => keepalive_topic
    _last_seen: Dict[str, datetime] = field(default_factory=dict, init=False)   # remote_identifier => datetime
    _last_keepalive_sent: Dict[str, datetime] = field(default_factory=dict, init=False)   # remote_identifier => datetime

    _connections: Dict[str, Dict[str, Connection]] = field(default_factory=lambda: defaultdict(dict), init=False)    # remote_identifier => { operation_name => Connection }
    _pending_requests: Dict[str, Dict[str, PendingRequest]] = field(default_factory=lambda: defaultdict(dict), init=False)    # remote_identifier => { operation_name => PendingRequest }

    _unsubscribe_on_full_close_bucket: Dict[str, Set[str]] = field(default_factory=lambda: defaultdict(set), init=False)

    __stop_loop_controller: asyncio.Event = field(default_factory=lambda: asyncio.Event(), init=False)
    __pause_loop_controller: asyncio.Event = field(default_factory=lambda: asyncio.Event(), init=False)

    def __post_init__(self):
        if 0 > self.send_keepalive_before_timelimit:
            raise ValueError("send_keepalive_before_timelimit must be >= 0")

    @property
    def keepalive_request_topic(self) -> str:
        return f"$keepalive.{self.identifier}.request"

    @property
    def keepalive_topic(self) -> str:
        return f"$keepalive.{self.identifier}"

    @property
    def _all_pending_requests(self) -> List[PendingRequest]:

        pending_requests: List[PendingRequest] = []
        for of_operation in self._pending_requests.values():
            for pending_request in of_operation.values():
                pending_requests.append(pending_request)

        return pending_requests

    @property
    def _all_connections(self) -> List[Connection]:

        connections: List[Connection] = []
        for of_operation in self._connections.values():
            for connection in of_operation.values():
                connections.append(connection)

        return connections

    @property
    def dead_remote_identifiers(self) -> List[str]:
        if self.consider_others_dead_after is None:
            return []

        dead = []
        now = datetime.now()
        for remote_identifier, last_seen in self._last_seen.items():
            if (last_seen + timedelta(seconds=self.consider_others_dead_after)) < now:
                dead.append(remote_identifier)

        return dead


    async def _get_on_close_data(self, remote_identifier: str, operation_name: str) -> Optional[bytes]:
        """
        Hook used to obtain data to send on close connection, by default None is returned
        """

        return None


    async def start(self, *args, **kwargs):
        logging.info(f"{self}: starting...")
        await self._on_starting(*args, **kwargs)
        await self._internal_start(*args, **kwargs)
        await self._on_started(*args, **kwargs)
        logging.info(f"{self}: started")


    async def _on_starting(self, *args, **kwargs):
        """
        Hook called before starting
        """

    async def _internal_start(self, *args, **kwargs):
        """
        Actual implementation to start the orbiter
        """

        await self.eventbus_client.connect()

        await asyncio.gather(
            self.eventbus_client.subscribe(
                self.keepalive_request_topic,
                self.__keepalive_request_event_handler
            ),
            self.eventbus_client.subscribe(
                self.keepalive_topic,
                self.__keepalive_event_handler
            )
        )

        if self.with_loop:
            self.start_loop()

    async def _on_started(self, *args, **kwargs):
        """
        Hook called after starting
        """

    async def stop(self, *args, **kwargs):
        logging.info(f"{self}: stopping...")
        await self._on_stopping(*args, **kwargs)
        await self._internal_stop(*args, **kwargs)
        await self._on_stopped(*args, **kwargs)
        logging.info(f"{self}: stopped")


    async def _on_stopping(self, *args, **kwargs):
        """
        Hook called before stopping
        """

    async def _internal_stop(self, *args, **kwargs):
        """
        Actual implementation to stop the orbiter
        """

        tasks = []

        tasks.append(
            asyncio.create_task(
                self.eventbus_client.multi_unsubscribe([
                    self.keepalive_request_topic,
                    self.keepalive_topic,
                ])
            )
        )

        for connection in self._all_connections:
            tasks.append(
                asyncio.create_task(
                    self.send_graceless_close_connection(
                        remote_identifier=connection.remote_identifier,
                        operation_name=connection.operation_name,
                        data=await self._get_on_close_data(connection.remote_identifier, connection.operation_name)
                    )
                )
            )

        self.stop_loop()

        await asyncio.gather(*tasks)

    async def _on_stopped(self, *args, **kwargs):
        """
        Hook called after stopping
        """

    def _connections_by_remote_identifier(self, remote_identifier: str) -> Dict[str, Connection]:
        return self._connections[remote_identifier]

    def _add_connection(self, connection: Connection):
        self._connections[connection.remote_identifier][connection.operation_name] = connection

    def _remove_connection(self, connection: Connection) -> Optional[Connection]:
        if connection.remote_identifier in self._connections:
            if connection.operation_name in self._connections[connection.remote_identifier]:
                return self._connections[connection.remote_identifier].pop(connection.operation_name)

        raise ValueError(f"{self}: no connection for identifier '{connection.remote_identifier}' and operation '{connection.operation_name}'")

    def _pending_requests_by_remote_identifier(self, remote_identifier: str) -> Dict[str, PendingRequest]:
        return self._pending_requests[remote_identifier]

    def _is_pending(self, remote_identifier: str, operation_name: str) -> bool:
        if remote_identifier in self._pending_requests:
            if operation_name in self._pending_requests[remote_identifier]:
                return True

        return False

    def _add_pending_request(self, pending_request: PendingRequest):
        self._pending_requests[pending_request.remote_identifier][pending_request.operation_name] = pending_request

    def _remove_pending_request(self, pending_request: PendingRequest) -> Optional[PendingRequest]:
        if pending_request.remote_identifier in self._pending_requests:
            if pending_request.operation_name in self._pending_requests[pending_request.remote_identifier]:
                return self._pending_requests[pending_request.remote_identifier].pop(pending_request.operation_name)

        raise ValueError(f"{self}: no pending request for identifier '{pending_request.remote_identifier}' and operation '{pending_request.operation_name}'")


    def retrieve_connections(self, *, remote_identifier: Optional[str] = None, input_topic: Optional[str] = None,
                             output_topic: Optional[str] = None, operation_name: Optional[str] = None,
                             input: Optional[Input] = None, output: Optional[Output] = None) -> List[Connection]:
        """
        Retrieve all connections which satisfy query
        """

        connections: List[Connection] = []

        for remote_identifier_, operation_name_connection in self._connections.items():
            if remote_identifier is not None and remote_identifier != remote_identifier_:
                continue

            for operation_name_, connection in operation_name_connection.items():
                assert operation_name_ == connection.operation_name

                if operation_name is not None and operation_name != operation_name_:
                    continue

                if input_topic is not None and input_topic != connection.input_topic:
                    continue

                if output_topic is not None and output_topic != connection.output_topic:
                    continue

                if input is not None and not input.is_compatible(connection.input):
                    continue

                if output is not None and not output.is_compatible(connection.output):
                    continue

                connections.append(connection)

        return connections

    def _find_connection_or_fail(self, input_topic: str, operation_name: str) -> Connection:
        """
        Find the connection associated to given input topic and operation name.
        Raise ValueError if too many connections are found or if there are no connections.
        """

        connections = self.retrieve_connections(
            input_topic=input_topic,
            operation_name=operation_name
        )

        if len(connections) <= 0:
            raise ValueError("Connection not found")

        if len(connections) >= 2:
            raise ValueError("Too many connections found")

        return connections[0]

    async def _retrieve_and_touch_connections(self, operation_name: str, *, input_topic: Optional[str] = None) -> List[Connection]:
        """
        Retrieve connections based on operation's name and input topic, then *lock* and touch connections.
        Finally, connections are returned.
        """

        connections = self.retrieve_connections(
            input_topic=input_topic,
            operation_name=operation_name
        )

        for connection in connections:
            async with connection.lock:
                connection.touch()

        return connections

    def _on_promote_pending_request_to_connection(self, pending_request: PendingRequest):
        """
        Hook called before promotion
        """

    def _promote_pending_request_to_connection(self, pending_request: PendingRequest):
        """
        Transform a pending request into a connection
        """

        try:

            self._on_promote_pending_request_to_connection(pending_request)

            self._add_connection(pending_request.into_connection())
            self._remove_pending_request(pending_request)

        except Exception as e:
            logging.error(f"{self}: {repr(e)}")

            if self.raise_exceptions:
                raise e

    async def discard_expired_pending_requests(self) -> int:
        """
        Remove expired pending requests.
        Return total amount of discarded requests
        """

        if self.pending_requests_expire_after is None:
            return 0

        if len(self._pending_requests) == 0:
            return 0

        to_remove: List[PendingRequest] = []

        discarded = 0
        for remote_identifier, of_operation in self._pending_requests.items():
            for operation_name, pending_request in of_operation.items():
                if (pending_request.created_at + timedelta(seconds=self.pending_requests_expire_after)) < datetime.now():
                    to_remove.append(pending_request)

        for pending_request in to_remove:
            async with pending_request.lock:
                try:
                    self._remove_pending_request(pending_request)
                    discarded += 1
                except Exception as e:
                    logging.warning(f"{self}: pending request {pending_request} was removed before discarding")

        return discarded

    async def close_unused_connections(self) -> int:
        """
        Send a graceful close request to all remote orbiter if connection was unused based on close_connection_if_unused_after
        """

        try:
            if self.close_connection_if_unused_after is None:
                return 0

            if len(self._connections) == 0:
                return 0

            to_close: List[Connection] = []

            closed = 0
            for remote_identifier, of_operation in self._connections.items():
                for operation_name, connection in of_operation.items():
                    expiration= connection.created_at + timedelta(seconds=self.close_connection_if_unused_after)
                    if expiration < datetime.now():
                        to_close.append(connection)

            tasks = []
            for connection in to_close:
                tasks.append(
                    asyncio.create_task(
                        self.send_graceful_close_connection(connection.remote_identifier, connection.operation_name)
                    )
                )
                closed += 1

            if len(tasks) > 0:
                await asyncio.gather(*tasks)

            return closed

        except Exception as e:
            logging.error(f"{self}: {repr(e)}")

            if self.raise_exceptions:
                raise e

        return 0

    async def force_close_connection_for_out_to_timeout_pending_graceful_close_connection(self) -> int:
        try:
            if self.graceful_close_timeout is None:
                return 0

            if len(self._connections) == 0:
                return 0

            to_close: List[Connection] = []

            closed = 0
            for remote_identifier, of_operation in self._connections.items():
                for operation_name, connection in of_operation.items():
                    if connection.soft_closed_at is None:
                        continue

                    expiration= connection.soft_closed_at + timedelta(seconds=self.graceful_close_timeout)
                    if expiration < datetime.now():
                        to_close.append(connection)

            tasks = []
            for connection in to_close:
                tasks.append(
                    asyncio.create_task(
                        self.send_graceless_close_connection(connection.remote_identifier, connection.operation_name)
                    )
                )
                closed += 1

            if len(tasks) > 0:
                await asyncio.gather(*tasks)

            return closed

        except Exception as e:
            logging.error(f"{self}: {repr(e)}")

            if self.raise_exceptions:
                raise e

        return 0

    def update_acquaintances(self, remote_identifier: str,
        *, keepalive_topic: str, keepalive_request_topic: str, consider_me_dead_after: float):
        """
        Update knowledge about keepalive request topics, keepalive topics and dead time
        """

        self._remote_keepalive_request_topics[remote_identifier] = keepalive_request_topic

        self._remote_keepalive_topics[remote_identifier] = keepalive_topic

        self._others_considers_me_dead_after[remote_identifier] = consider_me_dead_after

    def have_seen(self, remote_identifier: str, *, when: Optional[datetime] = None):
        """
        Update last seen for remote orbiter
        """

        if when is None:
            when = datetime.now()

        self._last_seen[remote_identifier] = when

    def clear_last_seen(self):
        self._last_seen = dict()

    async def _on_keepalive_request(self, from_identifier: str):
        """
        Hook called on keepalive request, before response
        """

    @event_handler
    async def __keepalive_request_event_handler(self, topic: str, event: Event[KeepaliveRequestMessage]):
        try:
            await self._on_keepalive_request(event.payload.from_identifier)

            await self.eventbus_client.publish(
                event.payload.keepalive_topic,
                KeepaliveMessage(from_identifier=self.identifier)
            )
        except Exception as e:
            logging.error(f"{self}: {repr(e)}")

            if self.raise_exceptions:
                raise e

    async def _on_keepalive(self, from_identifier: str):
        """
        Hook called on inbound keepalive
        """

    @event_handler
    async def __keepalive_event_handler(self, topic: str, event: Event[KeepaliveMessage]):
        await self._on_keepalive(event.payload.from_identifier)

        self._last_seen[event.payload.from_identifier] = datetime.now()

    async def send_keepalive(self, remote_identifier: str):

        if remote_identifier not in self._remote_keepalive_topics:
            raise ValueError(f"Keepalive topic not found for {remote_identifier}")

        await self.eventbus_client.publish(
            self._remote_keepalive_topics[remote_identifier],
            KeepaliveMessage(
                from_identifier=self.identifier
            )
        )

        self._last_keepalive_sent[remote_identifier] = datetime.now()

    async def send_keepalive_request(self, *, keepalive_request_topic: Optional[str] = None, remote_identifier: Optional[str] = None):

        if keepalive_request_topic is None and remote_identifier is None:
            raise ValueError("Missed target")

        if remote_identifier is not None:
            keepalive_request_topic = self._remote_keepalive_request_topics[remote_identifier]

        await self.eventbus_client.publish(
            keepalive_request_topic,
            KeepaliveRequestMessage(
                from_identifier=self.identifier,
                keepalive_topic=self.keepalive_topic
            )
        )

    async def send_all_keepalive_based_on_connections(self):
        """
        Send keepalive messages to all remote orbiters which have a connection with this orbiter
        """

        tasks = []
        for remote_identifier, operations in self._connections.values():
            if len(operations) > 0 and remote_identifier in self._remote_keepalive_topics:
                tasks.append(
                    asyncio.create_task(self.send_keepalive(remote_identifier=remote_identifier))
                )

        await asyncio.gather(*tasks)

    async def send_keepalive_based_on_connections_and_threshold(self):
        """
        Send keepalive messages to all remote orbiters which have a connection with this orbiter only if
        send_keepalive_before_timelimit seconds away from being considered dead this orbiter
        """

        tasks = []

        for remote_identifier in self._connections.keys():
            if remote_identifier not in self._others_considers_me_dead_after:
                logging.error(f"{self}: no dead time associated to {remote_identifier}, keepalive sending skipped")
                continue

            if remote_identifier not in self._last_keepalive_sent:
                logging.debug(f"{self}: not previous keepalive sent to {remote_identifier}")
                tasks.append(
                    asyncio.create_task(
                        self.send_keepalive(remote_identifier)
                    )
                )
                continue

            considered_dead_at: datetime = self._last_keepalive_sent[remote_identifier] + timedelta(
                seconds=self._others_considers_me_dead_after[remote_identifier])

            if (considered_dead_at - datetime.now()).seconds < 0:
                logging.warning(f"{self}: {remote_identifier} could be flag me as dead, anyway keepalive will be sent")

            assert 0 <= self.send_keepalive_before_timelimit, "send_keepalive_threshold_multiplier must be >= 0"

            if remote_identifier not in self._last_keepalive_sent \
                    or (considered_dead_at - datetime.now()).seconds < self.send_keepalive_before_timelimit:
                tasks.append(
                    asyncio.create_task(
                        self.send_keepalive(remote_identifier)
                    )
                )

        await asyncio.gather(*tasks)


    def _build_incoming_close_connection_topic(self, remote_identifier: str, operation_name: str) -> str:
        return f"{operation_name}.{self.identifier}.{remote_identifier}.close"

    async def send_graceless_close_connection(self, remote_identifier: str, operation_name: str, data: Optional[bytes] = None):
        """
        Send a graceless close connection request to specified remote orbiter. Therefore, self side connection will be closed immediately
        """

        try:

            await self._on_graceless_close_connection(remote_identifier, operation_name, data)

            connection = await self._close_self_side_connection(
                remote_identifier,
                operation_name
            )

            await self.eventbus_client.publish(
                connection.close_connection_to_remote_topic,
                GracelessCloneConnectionMessage(
                    from_identifier=self.identifier,
                    operation_name=operation_name,
                    data=data
                )
            )

        except Exception as e:
            logging.error(f"{self}: {repr(e)}")

            if self.raise_exceptions:
                raise e

    async def _on_graceless_close_connection(self, remote_identifier: str, operation_name: str, data: Optional[bytes]):
        """
        Hook called before graceless close connection request is sent
        """

    async def _on_close_connection(self, connection: Connection):
        """
        Hook called when a connection is closed
        """

    async def _close_self_side_connection(self, remote_identifier: str, operation_name: str):
        """
        Close local connection with remote orbiter, therefore only this orbiter will no longer be able to use connection.
        Generally, a close connection request was sent before this method call.
        """

        connection = self._connections[remote_identifier][operation_name]

        async with connection.lock:
            connection = self._remove_connection(connection)

        await self._on_close_connection(connection)

        close_incoming_close_connection_task = self.eventbus_client.unsubscribe(connection.incoming_close_connection_topic)

        if len(self._connections[remote_identifier].values()) == 0:
            await self.eventbus_client.multi_unsubscribe(list(self._unsubscribe_on_full_close_bucket[remote_identifier]), parallelize=True)
            self._unsubscribe_on_full_close_bucket.pop(remote_identifier, None)

        await close_incoming_close_connection_task

        logging.info(f"{self}: self side connection {connection} closed")

        return connection

    def _build_ack_close_topic(self, remote_identifier: str, operation_name: str) -> str:
        return f"{operation_name}.{self.identifier}.{remote_identifier}.close.ack"

    async def send_graceful_close_connection(self, remote_identifier: str, operation_name: str, data: Optional[bytes] = None):
        """
        Send a graceful close connection request to specified remote orbiter, therefore self side connection is not close immediately, but ACK is waited
        """

        try:
            await self._on_graceful_close_connection(remote_identifier, operation_name, data)

            connections = self.retrieve_connections(
                remote_identifier=remote_identifier,
                operation_name=operation_name
            )

            assert len(connections) == 1

            connection = connections[0]

            close_connection_to_remote_topic = None
            async with connection.lock:
                connection.soft_close()
                close_connection_to_remote_topic = connection.close_connection_to_remote_topic

            ack_topic = self._build_ack_close_topic(remote_identifier, operation_name)

            await self.eventbus_client.subscribe(ack_topic, self.__close_connection_ack_event_handler)

            assert close_connection_to_remote_topic is not None

            await self.eventbus_client.publish(
                close_connection_to_remote_topic,
                GracefulCloseConnectionMessage(
                    from_identifier=self.identifier,
                    operation_name=operation_name,
                    ack_topic=ack_topic,
                    data=data
                )
            )

        except Exception as e:
            logging.error(f"{self}: {repr(e)}")

            if self.raise_exceptions:
                raise e

    async def _on_graceful_close_connection(self, remote_identifier: str, operation_name: str, data: Optional[bytes]):
        """
        Hook called before sending graceful close connection request
        """

    async def _graceful_close_connection(self, topic: str, close_connection_message: GracefulCloseConnectionMessage):
        await self._on_graceful_close_connection(
            close_connection_message.from_identifier,
            close_connection_message.operation_name,
            close_connection_message.data
        )

        await self._close_self_side_connection(
            close_connection_message.from_identifier,
            close_connection_message.operation_name
        )

        await self.eventbus_client.publish(
            close_connection_message.ack_topic,
            CloseConnectionAckMessage(
                from_identifier=self.identifier,
                operation_name=close_connection_message.operation_name
            )
        )

    @event_handler
    async def __close_connection_ack_event_handler(self, topic: str, event: Event[CloseConnectionAckMessage]):

        self.have_seen(event.payload.from_identifier)

        await asyncio.gather(
            self._close_self_side_connection(
                event.payload.from_identifier,
                event.payload.operation_name
            ),
            self.eventbus_client.unsubscribe(topic)
        )

    @event_handler
    async def _close_connection_event_handler(self, topic: str, event: Event[GracefulCloseConnectionMessage | GracelessCloneConnectionMessage]):
        try:
            if isinstance(event.payload, GracefulCloseConnectionMessage):
                await self._graceful_close_connection(topic, event.payload)

            elif isinstance(event.payload, GracelessCloneConnectionMessage):
                await self._on_graceless_close_connection(
                    event.payload.from_identifier,
                    event.payload.operation_name,
                    event.payload.data
                )

                await self._close_self_side_connection(
                    event.payload.from_identifier,
                    event.payload.operation_name
                )

            else:
                logging.error(f"{self}: unable to handle close connection event: {event}")

        except Exception as e:
            logging.error(f"{self}: {repr(e)}")

            if self.raise_exceptions:
                raise e



    async def _on_loop_start(self):
        """
        Hook called on loop start
        """

    async def _on_new_loop_iteration(self):
        """
        Hook called before every loop iteration
        """

    async def _on_loop_iteration_end(self):
        """
        Hook called at the end of every loop iteration
        """

    async def _on_loop_iteration(self):
        """
        Hook called during every loop iteration
        """

    def start_loop(self):
        self.__stop_loop_controller.clear()
        self.__pause_loop_controller.clear()
        asyncio.create_task(self.__loop())

    def stop_loop(self):
        self.__stop_loop_controller.set()

    def pause_loop(self):
        self.__pause_loop_controller.set()

    def resume_loop(self):
        self.__pause_loop_controller.clear()

    async def __loop(self):

        await self._on_loop_start()

        while not self.__stop_loop_controller.is_set():

            await asyncio.sleep(self.loop_interval)

            if self.__pause_loop_controller.is_set():
                continue

            try:
                await self._on_new_loop_iteration()

                await asyncio.gather(
                    self._on_loop_iteration(),
                    self.close_unused_connections(),
                    self.discard_expired_pending_requests(),
                    self.force_close_connection_for_out_to_timeout_pending_graceful_close_connection(),
                    self.send_keepalive_based_on_connections_and_threshold(),
                )

                await self._on_loop_iteration_end()

            except Exception as e:

                logging.error(f"{self}: error during loop iteration: {repr(e)}")

                if self.raise_exceptions:
                    raise e
