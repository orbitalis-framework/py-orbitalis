from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, List


@dataclass
class Connection:
    """
    Orbiter2 uses this connection to execute related operation on Orbiter1.

    Orbiter1 <--- input_topic --- Orbiter2
    Orbiter1 --- output_topic ---> Orbiter2     (optional, only if there is an output)

    Orbiter (you) --- close_connection_remote_topic ---> Orbiter (remote)
    Orbiter (you) <--- close_connection_local_topic --- Orbiter (remote)

    Orbiter (you) <--- keepalive_topic --- Orbiter (remote)

    Author: Nicola Ricciardi
    """

    operation_name: str
    remote_identifier: str

    input_topic: str
    input_schemas: List[str]

    close_connection_local_topic: str
    close_connection_remote_topic: str
    keepalive_topic: str

    output_topic: Optional[str] = field(default=None)
    output_schemas: Optional[List[str]] = field(default=None)

    created_at: datetime = field(default_factory=lambda: datetime.now())
    last_use: Optional[datetime] = field(default=None)

    def touch(self):
        self.last_use = datetime.now()