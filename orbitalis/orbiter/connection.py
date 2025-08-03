import asyncio
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, List, Self

from orbitalis.orbiter.schemaspec import Input, Output


@dataclass
class Connection:
    """
    Orbiter2 uses this connection to execute related operation on Orbiter1.

    Orbiter1 <--- input_topic --- Orbiter2
    Orbiter1 --- output_topic ---> Orbiter2     (optional, only if there is an output)

    Orbiter (you) --- close_connection_to_remote_topic ---> Orbiter (remote)
    Orbiter (you) <--- close_connection_to_local_topic --- Orbiter (remote)

    Author: Nicola Ricciardi
    """

    operation_name: str
    remote_identifier: str

    incoming_close_connection_topic: str
    close_connection_to_remote_topic: str

    lock: asyncio.Lock = field(default_factory=lambda: asyncio.Lock(), init=False)

    input: Input
    output: Output

    input_topic: Optional[str] = field(default=None)
    output_topic: Optional[str] = field(default=None)

    soft_closed_at: Optional[datetime] = field(default=None, init=False)
    created_at: datetime = field(default_factory=lambda: datetime.now())
    last_use: Optional[datetime] = field(default=None)

    @property
    def is_soft_closed(self) -> bool:
        return self.soft_closed_at is not None

    @property
    def has_input(self) -> bool:
        return self.input_topic is not None

    @property
    def has_output(self) -> bool:
        return self.output_topic is not None

    def touch(self):
        """
        Update last use
        """
        self.last_use = datetime.now()

    def soft_close(self):
        if self.soft_closed_at is None:
            self.soft_closed_at = datetime.now()

    def __str__(self):
        return f"('{self.remote_identifier}', '{self.operation_name}')"