import asyncio
import random
from dataclasses import dataclass, field
from typing import override, Optional
from datetime import datetime
from busline.event.event import Event
from busline.event.message.number_message import Int64Message

from orbitalis.orbiter.schemaspec import Input, Output
from orbitalis.plugin.operation import operation, Operation, Policy
from orbitalis.plugin.plugin import Plugin


@dataclass
class RandomNumberPlugin(Plugin):
    """
    This plugin has only one operation which sends periodically a random number
    """

    last_sent: Optional[datetime] = field(default=None)     # will be used to check if a new value must be sent

    def __post_init__(self):
        super().__post_init__()

        # Manually define "randint" operation
        self.operations["randint"] = Operation(
            name="randint",     # operation's name
            input=Input.no_input(),     # no inputs are expected
            handler=None,               # handler is not needed, given that no inputs must be processed
            output=Output.from_message(Int64Message),   # integers will be sent to cores
            policy=Policy.no_constraints()          # no constraint during handshake
        )

    @override
    async def _on_loop_iteration(self):     # we use loop iteration hook to provide periodic operations

        now = datetime.now()

        # Only if the enough time is elapsed the operation will be executed
        if self.last_sent is None or (now - self.last_sent).seconds > 2:    # send a new random int every 2 seconds
            self.last_sent = now    # update timer for next iteration
            await self.__send_randint()


    async def __send_randint(self):
        random_number = random.randint(0, 100)      # generate a new random number, it will be sent

        connections = await self._retrieve_and_touch_connections(operation_name="randint")  # retrieve current core connections

        tasks = []
        for connection in connections:
            if connection.has_output:   # check if output is expected (it should be...)
                tasks.append(
                    asyncio.create_task(
                        self.eventbus_client.publish(   # send random int
                            connection.output_topic,
                            random_number
                        )
                    )
                )

        await asyncio.gather(*tasks)



