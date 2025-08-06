import asyncio
import random
from dataclasses import dataclass, field
from typing import override, Optional
from datetime import datetime

from busline.event.message.string_message import StringMessage

from orbitalis.orbiter.schemaspec import Input, Output
from orbitalis.plugin.operation import operation, Operation, Policy
from orbitalis.plugin.plugin import Plugin


@dataclass
class HelloSenderPlugin(Plugin):
    """
    This plugin has only one operation which sends a "hello" message periodically
    """

    __stop_custom_loop: asyncio.Event = field(default_factory=lambda: asyncio.Event())

    def __post_init__(self):
        super().__post_init__()

        # Manually define "hello" operation
        self.operations["hello"] = Operation(
            name="hello",     # operation's name
            input=Input.no_input(),     # no inputs are expected
            handler=None,               # handler is not needed, given that no inputs must be processed
            output=Output.from_message(StringMessage),   # "hello" string will be sent to cores
            policy=Policy.no_constraints()          # no constraint during handshake
        )

    @override
    async def _internal_start(self, *args, **kwargs):
        await super()._internal_start(*args, **kwargs)

        self.__stop_custom_loop.clear()     # allow to start custom loop

        # Start custom loop task
        asyncio.create_task(        # created task is ignored, because __stop_custom_loop is used
            self.__custom_loop()
        )

    @override
    async def _internal_stop(self, *args, **kwargs):
        await super()._internal_stop(*args, **kwargs)

        # During plugin stop, custom loop must be stopped too
        self.__stop_custom_loop.set()

    async def __custom_loop(self):
        while not self.__stop_custom_loop.is_set():
            await asyncio.sleep(2)      # to prevent spamming, custom loop is paused for 2 seconds

            await self.__send_hello()

    async def __send_hello(self):
        connections = await self._retrieve_and_touch_connections(operation_name="hello")  # retrieve current core connections

        tasks = []
        for connection in connections:
            if connection.has_output:   # check if output is expected (it should be...)
                tasks.append(
                    asyncio.create_task(
                        self.eventbus_client.publish(   # send random int
                            connection.output_topic,
                            "hello"     # hello message
                        )
                    )
                )

        await asyncio.gather(*tasks)



