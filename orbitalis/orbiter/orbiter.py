import json
import logging
from abc import ABC
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any, Set, Coroutine
from busline.client.pubsub_client import PubTopicSubClient
import uuid

from busline.client.subscriber.topic_subscriber.event_handler import event_handler
from busline.event.event import Event
from orbitalis.events.close_connection import GracefulCloneConnectionMessage, GracelessCloneConnectionMessage, \
    CloseConnectionAckMessage
from orbitalis.orbiter.connection import Connection
from orbitalis.orbiter.pending_request import PendingRequest


DEFAULT_DISCOVER_TOPIC = "$handshake.discover"


@dataclass(kw_only=True)
class Orbiter(ABC):
    """

    Author: Nicola Ricciardi
    """

    eventbus_client: PubTopicSubClient

    identifier: str = field(default_factory=lambda: str(uuid.uuid4()))

    discover_topic: str = field(default=DEFAULT_DISCOVER_TOPIC)
    parallelize: bool = field(default=True)
    raise_exceptions: bool = field(default=False)
    undefined_is_compatible: bool = False

    related_subscribed_topics: Dict[str, Set[str]] = field(default_factory=lambda: defaultdict(set))   # remote_identifier => topics
    connections: Dict[str, Dict[str, Connection]] = field(default_factory=lambda: defaultdict(dict), init=False)    # remote_identifier => { operation_name => Connection }
    pending_requests: Dict[str, Dict[str, PendingRequest]] = field(default_factory=lambda: defaultdict(dict), init=False)    # remote_identifier => { operation_name => PendingRequest }


    def connections_by_remote_identifier(self, remote_identifier: str) -> Dict[str, Connection]:
        return self.connections[remote_identifier]

    def _add_connection(self, remote_identifier: str, operation_name: str, connection: Connection):
        self.connections[remote_identifier][operation_name] = connection

    def _remove_connection(self, remote_identifier: str, operation_name: str) -> Optional[Connection]:
        if remote_identifier in self.connections:
            if operation_name in self.connections[remote_identifier]:
                return self.connections[remote_identifier].pop(operation_name)

        logging.warning(f"{self}: no connection for identifier '{remote_identifier}' and operation '{operation_name}'")

        return None

    def pending_requests_by_remote_identifier(self, remote_identifier: str) -> Dict[str, PendingRequest]:
        return self.pending_requests[remote_identifier]

    def _add_pending_request(self, remote_identifier: str, operation_name: str, pending_request: PendingRequest):
        self.pending_requests[remote_identifier][operation_name] = pending_request

    def _remove_pending_request(self, remote_identifier: str, operation_name: str) -> Optional[PendingRequest]:
        if remote_identifier in self.pending_requests:
            if operation_name in self.pending_requests[remote_identifier]:
                return self.pending_requests[remote_identifier].pop(operation_name)

        logging.warning(f"{self}: no pending request for identifier '{remote_identifier}' and operation '{operation_name}'")

        return None

    def retrieve_connections(self, *, remote_identifier: Optional[str] = None, input_topic: Optional[str] = None, output_topic: Optional[str] = None, operation_name: Optional[str] = None) -> List[Connection]:
        # TODO: cache?

        connections: List[Connection] = []

        for remote_identifier_, operation_name_connection in self.connections.items():
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

    async def _on_promote_pending_request_to_connection(self, remote_identifier: str, operation_name: str):
        """
        TODO: doc
        """

    async def promote_pending_request_to_connection(self, remote_identifier: str, operation_name: str):
        try:
            pending_request = self.pending_requests_by_remote_identifier(remote_identifier)[operation_name]

            await self._on_promote_pending_request_to_connection(remote_identifier, operation_name)

            self._add_connection(remote_identifier, operation_name, pending_request.into_connection())
            self._remove_pending_request(remote_identifier, operation_name)

        except Exception as e:
            logging.error(f"{self}: {e}")

            if self.raise_exceptions:
                raise e

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
                    self.identifier,
                    operation_name,
                    data
                ).into_event()
            )

        except Exception as e:
            logging.error(f"{self}: {e}")

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
            connection = self._remove_connection(
                remote_identifier,
                operation_name
            )

            if connection is None:
                raise ValueError(f"Connection not found for '{remote_identifier}', '{operation_name}'")

            await self._on_close_connection(connection)


            if len(self.connections[remote_identifier].values()) == 0:
                await self.eventbus_client.multi_unsubscribe(list(self.related_subscribed_topics[remote_identifier]), parallelize=self.parallelize)
                self.related_subscribed_topics.pop(remote_identifier)

            logging.info(f"{self}: connection {connection} closed")

            return connection

        except Exception as e:
            logging.error(f"{self}: {e}")

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
            self.related_subscribed_topics[remote_identifier].add(ack_topic)

            await self.eventbus_client.subscribe(ack_topic, self._close_connection_ack_event_handler)

            await self.eventbus_client.publish(
                connection.close_connection_to_remote_topic,
                GracefulCloneConnectionMessage(
                    self.identifier,
                    operation_name,
                    ack_topic,
                    data
                ).into_event()
            )

        except Exception as e:
            logging.error(f"{self}: {e}")

            if self.raise_exceptions:
                raise e

    async def _on_graceful_close_connection(self, remote_identifier: str, operation_name: str, data: Optional[Any]):
        """
        TODO: doc
        """

    async def _graceful_close_connection_event_handler(self, topic: str, close_connection_message: GracefulCloneConnectionMessage):
        await self._on_graceful_close_connection(
            close_connection_message.remote_identifier,
            close_connection_message.operation_name,
            close_connection_message.data
        )

        await self._close_connection(
            close_connection_message.remote_identifier,
            close_connection_message.operation_name
        )

        await self.eventbus_client.publish(
            close_connection_message.ack_topic,
            CloseConnectionAckMessage(
                self.identifier,
                close_connection_message.operation_name
            ).into_event()
        )

    @event_handler
    async def _close_connection_ack_event_handler(self, topic: str, event: Event[CloseConnectionAckMessage]):
        await self._close_connection(
            event.payload.remote_identifier,
            event.payload.operation_name
        )

    @event_handler
    async def _close_connection_event_handler(self, topic: str, event: Event[GracefulCloneConnectionMessage | GracelessCloneConnectionMessage]):
        try:
            if isinstance(event.payload, GracefulCloneConnectionMessage):
                await self._graceful_close_connection_event_handler(topic, event.payload)

            elif isinstance(event.payload, GracelessCloneConnectionMessage):
                await self._on_graceless_close_connection(
                    event.payload.remote_identifier,
                    event.payload.operation_name,
                    event.payload.data
                )

                await self._close_connection(
                    event.payload.remote_identifier,
                    event.payload.operation_name
                )

            else:
                logging.error(f"{self}: unable to handle close connection event: {event}")

        except Exception as e:
            logging.error(f"{self}: {e}")

            if self.raise_exceptions:
                raise e

    def discard_expired_pending_requests(self):
        """
        Remove from pending requests expired requests based on datetime provided or seconds elapsed.
        Seconds override expiration_date.
        Return total amount of discarded requests
        """

        raise NotImplemented()


    async def start(self, *args, **kwargs):
        logging.info(f"{self}: starting...")
        await self.on_starting(*args, **kwargs)
        await self._internal_start(*args, **kwargs)
        await self.on_started(*args, **kwargs)
        logging.info(f"{self}: started")


    async def on_starting(self, *args, **kwargs):
        """
        TODO
        """

    async def _internal_start(self, *args, **kwargs):
        """
        TODO
        """

        await self.eventbus_client.connect()

    async def on_started(self, *args, **kwargs):
        """
        TODO
        """

    async def stop(self, *args, **kwargs):
        logging.info(f"{self}: stopping...")
        await self.on_stopping(*args, **kwargs)
        await self._internal_stop(*args, **kwargs)
        await self.on_stopped(*args, **kwargs)
        logging.info(f"{self}: stopped")


    async def on_stopping(self, *args, **kwargs):
        """
        TODO
        """

    async def _internal_stop(self, *args, **kwargs):
        """
        TODO
        """

    async def on_stopped(self, *args, **kwargs):
        """
        TODO
        """

    def __repr__(self) -> str:
        return self.identifier


