from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, List

from orbitalis.orbiter.connection import Connection


@dataclass
class PendingRequest:
    operation_name: str
    remote_identifier: str

    offer_topic: Optional[str] = field(default=None, kw_only=True)
    reply_topic: Optional[str] = field(default=None, kw_only=True)
    response_topic: Optional[str] = field(default=None, kw_only=True)

    input_schemas: Optional[List[str]] = field(default=None)
    output_schemas: Optional[List[str]] = field(default=None)

    input_topic: Optional[str] = field(default=None, kw_only=True)
    close_connection_to_local_topic: Optional[str] = field(default=None, kw_only=True)
    close_connection_to_remote_topic: Optional[str] = field(default=None, kw_only=True)
    keepalive_to_local_topic: Optional[str] = field(default=None, kw_only=True)
    keepalive_to_remote_topic: Optional[str] = field(default=None, kw_only=True)
    output_topic: Optional[str] = field(default=None, kw_only=True)

    created_at: datetime = field(default_factory=lambda: datetime.now(), kw_only=True)
    updated_at: datetime = field(default_factory=lambda: datetime.now(), init=False)

    def into_connection(self) -> Connection:

        if self.input_topic is None:
            raise ValueError("input_topic missed")

        if self.input_schemas is None:
            raise ValueError("input_schemas missed")

        if self.close_connection_to_local_topic is None:
            raise ValueError("close_connection_to_local_topic missed")

        if self.close_connection_to_remote_topic is None:
            raise ValueError("close_connection_to_remote_topic missed")

        if self.keepalive_to_remote_topic is None:
            raise ValueError("keepalive_to_remote_topic missed")

        if  self.keepalive_to_local_topic is None:
            raise ValueError("keepalive_to_local_topic missed")

        if self.output_topic is not None and self.output_schemas is None:
            raise ValueError("output_schemas missed")


        return Connection(
            operation_name=self.operation_name,
            remote_identifier=self.remote_identifier,
            input_topic=self.input_topic,
            input_schemas=self.input_schemas,
            output_topic=self.output_topic,
            output_schemas=self.output_schemas,
            close_connection_to_local_topic=self.close_connection_to_local_topic,
            close_connection_to_remote_topic=self.close_connection_to_remote_topic,
            keepalive_to_local_topic=self.keepalive_to_local_topic,
            keepalive_to_remote_topic=self.keepalive_to_remote_topic,
        )
