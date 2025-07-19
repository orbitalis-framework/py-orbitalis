import asyncio
import json
import logging
from abc import ABC
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Set, Coroutine
from busline.client.pubsub_client import PubTopicSubClient
import uuid

from busline.client.subscriber.topic_subscriber.event_handler import event_handler
from busline.event.event import Event
from orbitalis.events.close_connection import GracefulCloneConnectionMessage, GracelessCloneConnectionMessage, \
    CloseConnectionAckMessage
from orbitalis.events.keepalive import KeepaliveRequestMessage, KeepaliveMessage
from orbitalis.orbiter.connection import Connection
from orbitalis.orbiter.pending_request import PendingRequest


DEFAULT_DISCOVER_TOPIC = "$handshake.discover"


DEFAULT_PENDING_REQUEST_EXPIRES_AFTER = 60.0
DEFAULT_DISCARD_CONNECTION_IF_UNUSED_AFTER = 600.0

DEFAULT_DISCARD_EXPIRED_PENDING_REQUESTS_INTERVAL = 0.2 * DEFAULT_PENDING_REQUEST_EXPIRES_AFTER
DEFAULT_DISCARD_UNUSED_CONNECTIONS_INTERVAL = 0.2 * DEFAULT_DISCARD_CONNECTION_IF_UNUSED_AFTER
DEFAULT_RENEW_KEEPALIVE_INTERVAL = 0.5

DEFAULT_KEEPALIVE_THRESHOLD_MULTIPLIER = 0.5 # %
DEFAULT_CONSIDERED_DEAD_AFTER = 120.0


@dataclass(kw_only=True)
class Orbiter(ABC):
    """

    Author: Nicola Ricciardi
    """

    eventbus_client: PubTopicSubClient

    identifier: str = field(default_factory=lambda: str(uuid.uuid4()))

    discover_topic: str = field(default=DEFAULT_DISCOVER_TOPIC)
    raise_exceptions: bool = field(default=False)

    main_loop_interval: float = field(default=0)
    discard_pending_requests_loop_interval: float = field(default=DEFAULT_DISCARD_EXPIRED_PENDING_REQUESTS_INTERVAL)
    discard_unused_connections_interval: float = field(default=DEFAULT_DISCARD_UNUSED_CONNECTIONS_INTERVAL)
    send_keepalive_interval: float = field(default=DEFAULT_RENEW_KEEPALIVE_INTERVAL)

    pending_request_expires_after: float = field(default=DEFAULT_PENDING_REQUEST_EXPIRES_AFTER)
    discard_connection_if_unused_after: float = field(default=DEFAULT_DISCARD_CONNECTION_IF_UNUSED_AFTER)
    pending_requests_expire_after: float = field(default=DEFAULT_CONSIDERED_DEAD_AFTER)
    consider_others_dead_after: float = field(default=DEFAULT_CONSIDERED_DEAD_AFTER)
    send_keepalive_threshold_multiplier: float = field(default=DEFAULT_KEEPALIVE_THRESHOLD_MULTIPLIER)
    connections_unused_after: Optional[float] = field(default=None)

    with_loops: bool = field(default=True)

    _others_considers_me_dead_after: Dict[str, float] = field(default_factory=dict, init=False)
    _remote_keepalive_request_topics: Dict[str, str] = field(default_factory=dict, init=False)   # remote_identifier => keepalive_request_topic
    _remote_keepalive_topics: Dict[str, str] = field(default_factory=dict, init=False)   # remote_identifier => keepalive_topic
    _last_seen: Dict[str, datetime] = field(default_factory=dict, init=False)   # remote_identifier => datetime
    _last_keepalive_sent: Dict[str, datetime] = field(default_factory=dict, init=False)   # remote_identifier => datetime

    _connections: Dict[str, Dict[str, Connection]] = field(default_factory=lambda: defaultdict(dict), init=False)    # remote_identifier => { operation_name => Connection }
    _pending_requests: Dict[str, Dict[str, PendingRequest]] = field(default_factory=lambda: defaultdict(dict), init=False)    # remote_identifier => { operation_name => PendingRequest }

    _unsubscribe_on_close_bucket: Dict[str, Set[str]] = field(default_factory=lambda: defaultdict(set), init=False)

    _stop_main_loop_controller: asyncio.Event = field(default_factory=lambda: asyncio.Event(), init=False)
    _pause_main_loop_controller: asyncio.Event = field(default_factory=lambda: asyncio.Event(), init=False)

    _stop_discard_pending_requests_loop_controller: asyncio.Event = field(default_factory=lambda: asyncio.Event(), init=False)
    _pause_discard_pending_requests_loop_controller: asyncio.Event = field(default_factory=lambda: asyncio.Event(), init=False)

    _stop_discard_connections_loop_controller: asyncio.Event = field(default_factory=lambda: asyncio.Event(), init=False)
    _pause_discard_connections_loop_controller: asyncio.Event = field(default_factory=lambda: asyncio.Event(), init=False)

    _stop_send_keepalive_loop_controller: asyncio.Event = field(default_factory=lambda: asyncio.Event(), init=False)
    _pause_send_keepalive_loop_controller: asyncio.Event = field(default_factory=lambda: asyncio.Event(), init=False)

    def __post_init__(self):
        if 0 > self.send_keepalive_threshold_multiplier or self.send_keepalive_threshold_multiplier > 1:
            raise ValueError("send_keepalive_threshold_multiplier must be between 0 and 1")

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

    def connections_by_remote_identifier(self, remote_identifier: str) -> Dict[str, Connection]:
        return self._connections[remote_identifier]

    def _add_connection(self, connection: Connection):
        self._connections[connection.remote_identifier][connection.operation_name] = connection

    def _remove_connection(self, remote_identifier: str, operation_name: str) -> Optional[Connection]:
        if remote_identifier in self._connections:
            if operation_name in self._connections[remote_identifier]:
                return self._connections[remote_identifier].pop(operation_name)

        logging.warning(f"{self}: no connection for identifier '{remote_identifier}' and operation '{operation_name}'")

        return None

    def pending_requests_by_remote_identifier(self, remote_identifier: str) -> Dict[str, PendingRequest]:
        return self._pending_requests[remote_identifier]

    def _is_pending(self, remote_identifier: str, operation_name: str) -> bool:
        if remote_identifier in self._pending_requests:
            if operation_name in self._pending_requests[remote_identifier]:
                return True

        return False

    def _add_pending_request(self, pending_request: PendingRequest):
        self._pending_requests[pending_request.remote_identifier][pending_request.operation_name] = pending_request

    def _remove_pending_request(self, remote_identifier: str, operation_name: str) -> Optional[PendingRequest]:
        if remote_identifier in self._pending_requests:
            if operation_name in self._pending_requests[remote_identifier]:
                return self._pending_requests[remote_identifier].pop(operation_name)

        logging.warning(f"{self}: no pending request for identifier '{remote_identifier}' and operation '{operation_name}'")

        return None

    def retrieve_connections(self, *, remote_identifier: Optional[str] = None, input_topic: Optional[str] = None, output_topic: Optional[str] = None, operation_name: Optional[str] = None) -> List[Connection]:
        # TODO: cache?

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

                connections.append(connection)

        return connections

    async def _on_promote_pending_request_to_connection(self, pending_request: PendingRequest):
        """
        TODO: doc
        """

    async def promote_pending_request_to_connection(self, pending_request: PendingRequest):
        async with asyncio.Lock():
            try:

                await self._on_promote_pending_request_to_connection(pending_request)

                self._add_connection(pending_request.into_connection())
                self._remove_pending_request(pending_request.remote_identifier, pending_request.operation_name)

            except Exception as e:
                logging.error(f"{self}: {repr(e)}")

                if self.raise_exceptions:
                    raise e

    async def discard_expired_pending_requests(self):
        """
        Remove from pending requests expired requests based on datetime provided or seconds elapsed.
        Seconds override expiration_date.
        Return total amount of discarded requests
        """

        for remote_identifier, of_operation in self._pending_requests.items():
            for operation_name, pending_request in of_operation.items():
                async with pending_request.lock:
                    if operation_name not in self._pending_requests:
                        continue    # someone has removed pending request before

                    self._remove_pending_request(remote_identifier, operation_name)

    async def discard_unused(self):
        """
        Remove from pending requests expired requests based on datetime provided or seconds elapsed.
        Seconds override expiration_date.
        Return total amount of discarded requests
        """

        for remote_identifier, of_operation in self._connections.items():
            for operation_name, connection in of_operation.items():
                async with connection.lock:
                    if operation_name not in self._connections:
                        continue  # someone has removed connection before

                    self._remove_connection(remote_identifier, operation_name)


    def update_acquaintances(self, remote_identifier: str,
        *, keepalive_topic: str, keepalive_request_topic: str, consider_me_dead_after: float):

        self._remote_keepalive_request_topics[remote_identifier] = keepalive_request_topic

        self._remote_keepalive_topics[remote_identifier] = keepalive_topic

    def have_seen(self, remote_identifier: str, *, when: Optional[datetime] = None):

        if when is None:
            when = datetime.now()

        self._last_seen[remote_identifier] = when

    def clear_last_seen(self):
        self._last_seen = dict()

    async def _on_keepalive_request(self, from_identifier: str):
        """
        TODO: doc
        """

    @event_handler
    async def _keepalive_request_event_handler(self, topic: str, event: Event[KeepaliveRequestMessage]):
        await self._on_keepalive_request(event.payload.from_identifier)

        await self.eventbus_client.publish(
            event.payload.keepalive_topic,
            KeepaliveMessage(from_identifier=self.identifier).into_event()
        )

    async def _on_keepalive(self, from_identifier: str):
        """
        TODO: doc
        """

    @event_handler
    async def _keepalive_event_handler(self, topic: str, event: Event[KeepaliveMessage]):
        await self._on_keepalive(event.payload.from_identifier)

        self._last_seen[event.payload.from_identifier] = datetime.now()

    async def send_keepalive(self, remote_identifier: str):

        if remote_identifier not in self._remote_keepalive_topics:
            raise ValueError(f"Keepalive topic not found for {remote_identifier}")

        await self.eventbus_client.publish(
            self._remote_keepalive_topics[remote_identifier],
            KeepaliveMessage(
                from_identifier=self.identifier
            ).into_event()
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
            ).into_event()
        )

    async def send_keepalive_based_on_connections(self):
        tasks = []
        for remote_identifier, operations in self._connections.values():
            if len(operations) > 0 and remote_identifier in self._remote_keepalive_topics:
                tasks.append(
                    asyncio.create_task(self.send_keepalive(remote_identifier=remote_identifier))
                )

        return asyncio.gather(*tasks)

    async def send_keepalive_based_on_connections_and_threshold(self):
        tasks = []
        for remote_identifier, operations in self._connections.items():
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

            considered_dead_at: datetime = self._last_keepalive_sent[remote_identifier] + timedelta(seconds=self._others_considers_me_dead_after[remote_identifier])

            if (considered_dead_at - datetime.now()).seconds < 0:
                logging.warning(f"{self}: {remote_identifier} could be flag me as dead, anyway keepalive sending is tried")

            assert 0 <= self.send_keepalive_threshold_multiplier <= 1, "send_keepalive_threshold_multiplier must be between 0 and 1"

            if remote_identifier not in self._last_keepalive_sent \
                    or (considered_dead_at - datetime.now()).seconds < (
                    self.send_keepalive_threshold_multiplier * self._others_considers_me_dead_after[remote_identifier]):
                tasks.append(
                    asyncio.create_task(
                        self.send_keepalive(remote_identifier)
                    )
                )

        await asyncio.gather(*tasks)


    def build_incoming_close_connection_topic(self, remote_identifier: str, operation_name: str) -> str:
        return f"{operation_name}.{self.identifier}.{remote_identifier}.close.{uuid.uuid4()}"

    async def graceless_close_connection(self, remote_identifier: str, operation_name: str, data: Optional[Any] = None):
        try:

            await self._on_graceless_close_connection(remote_identifier, operation_name, data)

            connection = await self._close_connection(
                remote_identifier,
                operation_name
            )

            await self.eventbus_client.publish(
                connection.close_connection_to_remote_topic,
                GracelessCloneConnectionMessage(
                    from_identifier=self.identifier,
                    operation_name=operation_name,
                    data=data
                ).into_event()
            )

        except Exception as e:
            logging.error(f"{self}: {repr(e)}")

            if self.raise_exceptions:
                raise e

    async def _on_graceless_close_connection(self, remote_identifier: str, operation_name: str, data: Optional[Any]):
        """
        TODO: doc
        """

    async def _on_close_connection(self, connection: Connection):
        """
        TODO: doc
        """

    async def _close_connection(self, remote_identifier: str, operation_name: str) -> Optional[Connection]:

        try:
            self.have_seen(remote_identifier)

            connection = self._remove_connection(
                remote_identifier,
                operation_name
            )

            if connection is None:
                raise ValueError(f"Connection not found for '{remote_identifier}', '{operation_name}'")

            await self._on_close_connection(connection)


            if len(self._connections[remote_identifier].values()) == 0:
                await self.eventbus_client.multi_unsubscribe(list(self._unsubscribe_on_close_bucket[remote_identifier]), parallelize=True)
                self._unsubscribe_on_close_bucket.pop(remote_identifier)

            logging.info(f"{self}: connection {connection} closed")

            return connection

        except Exception as e:
            logging.error(f"{self}: {repr(e)}")

            if self.raise_exceptions:
                raise e

        return None

    def build_ack_close_topic(self, remote_identifier: str, operation_name: str) -> str:
        return f"{operation_name}.{self.identifier}.{remote_identifier}.close.ack.{uuid.uuid4()}"

    async def graceful_close_connection(self, remote_identifier: str, operation_name: str, data: Optional[Any] = None):
        try:
            await self._on_graceful_close_connection(remote_identifier, operation_name, data)

            connections = self.retrieve_connections(
                remote_identifier=remote_identifier,
                operation_name=operation_name
            )

            assert len(connections) == 1

            connection = connections[0]

            ack_topic = self.build_ack_close_topic(remote_identifier, operation_name)
            self._unsubscribe_on_close_bucket[remote_identifier].add(ack_topic)

            await self.eventbus_client.subscribe(ack_topic, self._close_connection_ack_event_handler)

            await self.eventbus_client.publish(
                connection.close_connection_to_remote_topic,
                GracefulCloneConnectionMessage(
                    from_identifier=self.identifier,
                    operation_name=operation_name,
                    ack_topic=ack_topic,
                    data=data
                ).into_event()
            )

        except Exception as e:
            logging.error(f"{self}: {repr(e)}")

            if self.raise_exceptions:
                raise e

    async def _on_graceful_close_connection(self, remote_identifier: str, operation_name: str, data: Optional[Any]):
        """
        TODO: doc
        """

    async def _graceful_close_connection_event_handler(self, topic: str, close_connection_message: GracefulCloneConnectionMessage):
        await self._on_graceful_close_connection(
            close_connection_message.from_identifier,
            close_connection_message.operation_name,
            close_connection_message.data
        )

        await self._close_connection(
            close_connection_message.from_identifier,
            close_connection_message.operation_name
        )

        await self.eventbus_client.publish(
            close_connection_message.ack_topic,
            CloseConnectionAckMessage(
                from_identifier=self.identifier,
                operation_name=close_connection_message.operation_name
            ).into_event()
        )

    @event_handler
    async def _close_connection_ack_event_handler(self, topic: str, event: Event[CloseConnectionAckMessage]):
        await self._close_connection(
            event.payload.from_identifier,
            event.payload.operation_name
        )

    @event_handler
    async def _close_connection_event_handler(self, topic: str, event: Event[GracefulCloneConnectionMessage | GracelessCloneConnectionMessage]):
        try:
            if isinstance(event.payload, GracefulCloneConnectionMessage):
                await self._graceful_close_connection_event_handler(topic, event.payload)

            elif isinstance(event.payload, GracelessCloneConnectionMessage):
                await self._on_graceless_close_connection(
                    event.payload.from_identifier,
                    event.payload.operation_name,
                    event.payload.data
                )

                await self._close_connection(
                    event.payload.from_identifier,
                    event.payload.operation_name
                )

            else:
                logging.error(f"{self}: unable to handle close connection event: {event}")

        except Exception as e:
            logging.error(f"{self}: {repr(e)}")

            if self.raise_exceptions:
                raise e


    async def start(self, *args, **kwargs):
        logging.info(f"{self}: starting...")
        await self._on_starting(*args, **kwargs)
        await self._internal_start(*args, **kwargs)
        await self._on_started(*args, **kwargs)
        logging.info(f"{self}: started")


    async def _on_starting(self, *args, **kwargs):
        """
        TODO
        """

    async def _internal_start(self, *args, **kwargs):
        """
        TODO
        """

        await self.eventbus_client.connect()

        await self.eventbus_client.subscribe(
            self.keepalive_request_topic,
            self._keepalive_request_event_handler
        )

        await self.eventbus_client.subscribe(
            self.keepalive_topic,
            self._keepalive_event_handler
        )

        if self.with_loops:
            await self.start_loops()

    async def _on_started(self, *args, **kwargs):
        """
        TODO
        """

    async def stop(self, *args, **kwargs):
        logging.info(f"{self}: stopping...")
        await self._on_stopping(*args, **kwargs)
        await self._internal_stop(*args, **kwargs)
        await self._on_stopped(*args, **kwargs)
        logging.info(f"{self}: stopped")


    async def _on_stopping(self, *args, **kwargs):
        """
        TODO
        """

    async def _internal_stop(self, *args, **kwargs):
        """
        TODO
        """

        await self.eventbus_client.multi_unsubscribe([
            self.keepalive_request_topic,
            self.keepalive_topic,
        ])

        await self.stop_loops()

    async def _on_stopped(self, *args, **kwargs):
        """
        TODO
        """

    async def _on_main_loop_start(self):
        """
        TODO: doc
        """

    async def _on_main_loop_iteration(self):
        """
        TODO: doc
        """

    async def _main_loop(self):

        await self._on_main_loop_start()

        while not self._stop_main_loop_controller.is_set():
            while not self._pause_main_loop_controller.is_set():
                await asyncio.sleep(self.main_loop_interval)

                await self._on_main_loop_iteration()

    async def _on_discard_expired_pending_requests_loop_start(self):
        """
        TODO: doc
        """

    async def _on_new_discard_expired_pending_requests_loop_iteration(self):
        """
        TODO: doc
        """

    async def _on_discard_expired_pending_requests_loop_iteration_end(self):
        """
        TODO: doc
        """

    async def _discard_expired_pending_requests_loop(self):

        await self._on_discard_unused_loop_start()

        while not self._stop_main_loop_controller.is_set():
            while not self._pause_main_loop_controller.is_set():

                await asyncio.sleep(self.discard_pending_requests_loop_interval)

                await self._on_new_discard_expired_pending_requests_loop_iteration()

                await self.discard_expired_pending_requests()

                await self._on_discard_expired_pending_requests_loop_iteration_end()

    async def _on_discard_unused_loop_start(self):
        """
        TODO: doc
        """

    async def _on_new_discard_unused_loop_iteration(self):
        """
        TODO: doc
        """

    async def _on_discard_unused_loop_iteration_end(self):
        """
        TODO: doc
        """

    async def _discard_unused_loop(self):
        await self._on_discard_unused_loop_start()

        while not self._stop_main_loop_controller.is_set():
            while not self._pause_main_loop_controller.is_set():

                await asyncio.sleep(self.discard_unused_connections_interval)

                await self._on_new_discard_unused_loop_iteration()

                await self.discard_unused()

                await self._on_discard_unused_loop_iteration_end()

    async def _on_send_keepalive_loop_start(self):
        """
        TODO: doc
        """

    async def _on_send_keepalive_loop_iteration(self):
        """
        TODO: doc
        """

    async def _on_send_keepalive_loop_iteration_end(self):
        """
        TODO: doc
        """

    async def _send_keepalive_loop(self):
        await self._on_send_keepalive_loop_start()

        while not self._stop_send_keepalive_loop_controller.is_set():
            while not self._pause_send_keepalive_loop_controller.is_set():

                await asyncio.sleep(self.send_keepalive_interval)

                await self._on_send_keepalive_loop_iteration()

                await self.send_keepalive_based_on_connections_and_threshold()

                await self._on_send_keepalive_loop_iteration_end()

    async def start_loops(self):
        asyncio.create_task(self._main_loop())
        asyncio.create_task(self._discard_unused_loop())
        asyncio.create_task(self._discard_expired_pending_requests_loop())
        asyncio.create_task(self._send_keepalive_loop())

    async def stop_loops(self):
        self._stop_main_loop_controller.set()
        self._stop_discard_connections_loop_controller.set()
        self._stop_discard_pending_requests_loop_controller.set()
        self._stop_send_keepalive_loop_controller.set()

    async def pause_loops(self):
        self._pause_main_loop_controller.set()
        self._pause_discard_connections_loop_controller.set()
        self._pause_discard_pending_requests_loop_controller.set()
        self._pause_send_keepalive_loop_controller.set()
