from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, List

from orbitalis.orbiter.schemaspec import InputOutput


@dataclass
class Connection(InputOutput):
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

    input_topic: Optional[str] = field(default=None)

    # close_connection_to_local_topic: str
    # close_connection_to_remote_topic: str
    # keepalive_to_local_topic: str
    # keepalive_to_remote_topic: str

    output_topic: Optional[str] = field(default=None)

    created_at: datetime = field(default_factory=lambda: datetime.now())
    last_use: Optional[datetime] = field(default=None)

    def touch(self):
        self.last_use = datetime.now()