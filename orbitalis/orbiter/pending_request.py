from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, List

from orbitalis.orbiter.connection import Connection
from orbitalis.orbiter.schemaspec import SchemaSpec, Input, Output


@dataclass
class PendingRequest:
    operation_name: str
    remote_identifier: str

    offer_topic: Optional[str] = field(default=None, kw_only=True)
    reply_topic: Optional[str] = field(default=None, kw_only=True)
    response_topic: Optional[str] = field(default=None, kw_only=True)

    input: Input = field(default=None)
    output: Output = field(default=None)

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

        if self.input is None:
            raise ValueError("input missed")

        # TODO
        # if self.close_connection_to_local_topic is None:
        #     raise ValueError("close_connection_to_local_topic missed")
        #
        # if self.close_connection_to_remote_topic is None:
        #     raise ValueError("close_connection_to_remote_topic missed")
        #
        # if self.keepalive_to_remote_topic is None:
        #     raise ValueError("keepalive_to_remote_topic missed")
        #
        # if  self.keepalive_to_local_topic is None:
        #     raise ValueError("keepalive_to_local_topic missed")

        if self.output_topic is not None and self.output is None:
            raise ValueError("output missed")


        return Connection(
            operation_name=self.operation_name,
            remote_identifier=self.remote_identifier,
            input_topic=self.input_topic,
            input=self.input,
            output_topic=self.output_topic,
            output=self.output,
            # close_connection_to_local_topic=self.close_connection_to_local_topic,
            # close_connection_to_remote_topic=self.close_connection_to_remote_topic,
            # keepalive_to_local_topic=self.keepalive_to_local_topic,
            # keepalive_to_remote_topic=self.keepalive_to_remote_topic,
        )
