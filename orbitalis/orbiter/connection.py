from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, List

from orbitalis.orbiter.schemaspec import Input, Output


@dataclass
class Connection:
    """
    Orbiter2 uses this connection to execute related operation on Orbiter1.

    Orbiter1 <--- input_topic --- Orbiter2
    Orbiter1 --- output_topic ---> Orbiter2     (optional, only if there is an output)

    Orbiter (you) --- close_connection_to_remote_topic ---> Orbiter (remote)
    Orbiter (you) <--- close_connection_to_local_topic --- Orbiter (remote)

    Orbiter (you) <--- keepalive_to_local_topic --- Orbiter (remote)
    Orbiter (you) --- keepalive_to_remote_topic ---> Orbiter (remote)

    Author: Nicola Ricciardi
    """

    operation_name: str
    remote_identifier: str

    incoming_close_connection_topic: str
    close_connection_to_remote_topic: str

    input: Input
    output: Output

    input_topic: Optional[str] = field(default=None)
    output_topic: Optional[str] = field(default=None)

    created_at: datetime = field(default_factory=lambda: datetime.now())
    last_use: Optional[datetime] = field(default=None)

    def touch(self):
        """
        TODO: doc "why?"
        """
        self.last_use = datetime.now()

    def __str__(self):
        return f"('{self.remote_identifier}', '{self.operation_name}')"