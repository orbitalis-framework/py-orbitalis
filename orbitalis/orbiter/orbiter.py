import json
import logging
from abc import ABC
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict, List, Optional
from busline.client.pubsub_client import PubTopicSubClient
import uuid

from orbitalis.events.wellknown_topic import WellKnownTopic
from orbitalis.orbiter.connection import Connection
from orbitalis.orbiter.pending_request import PendingRequest


@dataclass(kw_only=True)
class Orbiter(ABC):
    """

    Author: Nicola Ricciardi
    """

    eventbus_client: PubTopicSubClient

    identifier: str = field(default_factory=lambda: str(uuid.uuid4()))

    discover_topic: str = field(default_factory=lambda: WellKnownTopic.discover_topic())

    raise_exceptions: bool = field(default=False)

    undefined_is_compatible: bool = False
    connections: Dict[str, Dict[str, Connection]] = field(default_factory=lambda: defaultdict(dict), init=False)    # remote_identifier => { operation_name => Connection }
    pending_requests: Dict[str, Dict[str, PendingRequest]] = field(default_factory=lambda: defaultdict(dict), init=False)    # remote_identifier => { operation_name => PendingRequest }


    def connections_by_remote_identifier(self, remote_identifier: str) -> Dict[str, Connection]:
        return self.connections[remote_identifier]

    def add_connection(self, remote_identifier: str, operation_name: str, connection: Connection):
        self.connections[remote_identifier][operation_name] = connection

    def pending_requests_by_remote_identifier(self, remote_identifier: str) -> Dict[str, PendingRequest]:
        return self.pending_requests[remote_identifier]

    def add_pending_request(self, remote_identifier: str, operation_name: str, pending_request: PendingRequest):
        self.pending_requests[remote_identifier][operation_name] = pending_request

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

    def promote_pending_request_to_connection(self, remote_identifier: str, operation_name: str):
        pending_request = self.pending_requests_by_remote_identifier(remote_identifier)[operation_name]

        self.add_connection(remote_identifier, operation_name, pending_request.into_connection())

    def remove_pending_request(self, remote_identifier: str, operation_name: str):
        if remote_identifier not in self.pending_requests or operation_name not in self.pending_requests[remote_identifier]:
            logging.warning(f"{self}: pending request not present for identifier '{remote_identifier}' and operation '{operation_name}'")
            return

        del self.pending_requests[remote_identifier][operation_name]

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


