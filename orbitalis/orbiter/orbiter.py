import asyncio
import json
import logging
from abc import ABC
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Set, Coroutine, Tuple
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


DEFAULT_LOOP_INTERVAL = 1
DEFAULT_PENDING_REQUESTS_EXPIRE_AFTER = 60.0

DEFAULT_SEND_KEEPALIVE_BEFORE_TIMELIMIT = 10.0
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

    loop_interval: float = field(default=DEFAULT_LOOP_INTERVAL)

    close_connection_if_unused_after: Optional[float] = field(default=None)
    pending_requests_expire_after: Optional[float] = field(default=DEFAULT_PENDING_REQUESTS_EXPIRE_AFTER)
    consider_others_dead_after: float = field(default=DEFAULT_CONSIDERED_DEAD_AFTER)
    send_keepalive_before_timelimit: float = field(default=DEFAULT_SEND_KEEPALIVE_BEFORE_TIMELIMIT)

    with_loop: bool = field(default=True)

    _others_considers_me_dead_after: Dict[str, float] = field(default_factory=dict, init=False)
    _remote_keepalive_request_topics: Dict[str, str] = field(default_factory=dict, init=False)   # remote_identifier => keepalive_request_topic
    _remote_keepalive_topics: Dict[str, str] = field(default_factory=dict, init=False)   # remote_identifier => keepalive_topic
    _last_seen: Dict[str, datetime] = field(default_factory=dict, init=False)   # remote_identifier => datetime
    _last_keepalive_sent: Dict[str, datetime] = field(default_factory=dict, init=False)   # remote_identifier => datetime

    _connections: Dict[str, Dict[str, Connection]] = field(default_factory=lambda: defaultdict(dict), init=False)    # remote_identifier => { operation_name => Connection }
    _pending_requests: Dict[str, Dict[str, PendingRequest]] = field(default_factory=lambda: defaultdict(dict), init=False)    # remote_identifier => { operation_name => PendingRequest }

    _unsubscribe_on_close_bucket: Dict[str, Set[str]] = field(default_factory=lambda: defaultdict(set), init=False)

    _stop_loop_controller: asyncio.Event = field(default_factory=lambda: asyncio.Event(), init=False)
    _pause_loop_controller: asyncio.Event = field(default_factory=lambda: asyncio.Event(), init=False)

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
    def pause_loop_controller(self) -> asyncio.Event:
        return self._pause_loop_controller

    @property
    def _all_connections(self) -> List[Connection]:

        connections: List[Connection] = []
        for of_operation in self._connections.values():
            for connection in of_operation.values():
                connections.append(connection)

        return connections

    @property
    def dead_remote_identifiers(self) -> List[str]:
        dead = []
        now = datetime.now()
        for remote_identifier, last_seen in self._last_seen.items():
            if (last_seen + timedelta(seconds=self.consider_others_dead_after)) < now:
                dead.append(remote_identifier)

        return dead

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


    def _retrieve_connections(self, *, remote_identifier: Optional[str] = None, input_topic: Optional[str] = None, output_topic: Optional[str] = None, operation_name: Optional[str] = None) -> List[Connection]:
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

    def _on_promote_pending_request_to_connection(self, pending_request: PendingRequest):
        """
        TODO: doc
        """

    def _promote_pending_request_to_connection(self, pending_request: PendingRequest):
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
        Remove from pending requests expired requests based on datetime provided or seconds elapsed.
        Seconds override expiration_date.
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

        """

        if self.close_connection_if_unused_after is None:
            return 0

        if len(self._connections) == 0:
            return 0

        to_close: List[Connection] = []

        closed = 0
        for remote_identifier, of_operation in self._connections.items():
            for operation_name, connection in of_operation.items():
                if (connection.created_at + timedelta(seconds=self.close_connection_if_unused_after)) < datetime.now():
                    to_close.append(connection)

        tasks = []
        for connection in to_close:
            tasks.append(
                asyncio.create_task(
                    self.graceful_close_connection(connection.remote_identifier, connection.operation_name)
                )
            )
            closed += 1

        if len(tasks) > 0:
            await asyncio.gather(*tasks)

        return closed


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

    async def send_all_keepalive_based_on_connections(self):
        tasks = []
        for remote_identifier, operations in self._connections.values():
            if len(operations) > 0 and remote_identifier in self._remote_keepalive_topics:
                tasks.append(
                    asyncio.create_task(self.send_keepalive(remote_identifier=remote_identifier))
                )

        return asyncio.gather(*tasks)

    async def send_keepalive_based_on_connections_and_threshold(self):
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
                logging.warning(
                    f"{self}: {remote_identifier} could be flag me as dead, anyway keepalive sending is tried")

            assert 0 <= self.send_keepalive_before_timelimit, "send_keepalive_threshold_multiplier must be >= 0"

            if remote_identifier not in self._last_keepalive_sent \
                    or (considered_dead_at - datetime.now()).seconds < self.send_keepalive_before_timelimit:
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

    async def _close_connection(self, remote_identifier: str, operation_name: str):

        try:
            self.have_seen(remote_identifier)

            connection = self._connections[remote_identifier][operation_name]

            async with connection.lock:
                connection = self._remove_connection(connection)

            await self._on_close_connection(connection)

            if len(self._connections[remote_identifier].values()) == 0:
                await self.eventbus_client.multi_unsubscribe(list(self._unsubscribe_on_close_bucket[remote_identifier]), parallelize=True)
                self._unsubscribe_on_close_bucket.pop(remote_identifier, None)

            logging.info(f"{self}: connection {connection} closed")

        except Exception as e:
            logging.error(f"{self}: {repr(e)}; maybe connection already removed")

            if self.raise_exceptions:
                raise e

    def build_ack_close_topic(self, remote_identifier: str, operation_name: str) -> str:
        return f"{operation_name}.{self.identifier}.{remote_identifier}.close.ack.{uuid.uuid4()}"

    async def graceful_close_connection(self, remote_identifier: str, operation_name: str, data: Optional[Any] = None):
        try:
            await self._on_graceful_close_connection(remote_identifier, operation_name, data)

            connections = self._retrieve_connections(
                remote_identifier=remote_identifier,
                operation_name=operation_name
            )

            assert len(connections) == 1

            connection = connections[0]

            ack_topic = self.build_ack_close_topic(remote_identifier, operation_name)

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
        return asyncio.gather(
            self._close_connection(
                event.payload.from_identifier,
                event.payload.operation_name
            ),
            self.eventbus_client.unsubscribe(topic)
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

        if self.with_loop:
            asyncio.create_task(self._loop())

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

        self._stop_loop_controller.set()

    async def _on_stopped(self, *args, **kwargs):
        """
        TODO
        """

    async def _on_loop_start(self):
        """
        TODO: doc
        """

    async def _on_new_loop_iteration(self):
        """
        TODO: doc
        """

    async def _on_loop_iteration_end(self):
        """
        TODO: doc
        """

    async def _on_loop_iteration(self):
        """
        TODO: doc
        """

    async def _loop(self):

        await self._on_loop_start()

        while not self._stop_loop_controller.is_set():
            while not self._pause_loop_controller.is_set():
                await asyncio.sleep(self.loop_interval)

                try:
                    await self._on_new_loop_iteration()

                    await asyncio.gather(
                        # self._on_loop_iteration(),
                        # self.close_unused_connections(),
                        # self.discard_expired_pending_requests(),
                        # self.send_keepalive_based_on_connections_and_threshold()
                    )

                    await self._on_loop_iteration_end()

                except Exception as e:

                    logging.error(f"{self}: error during loop iteration: {repr(e)}")

                    if self.raise_exceptions:
                        raise e
