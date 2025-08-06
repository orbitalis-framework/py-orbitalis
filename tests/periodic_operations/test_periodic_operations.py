import asyncio
import unittest
from dataclasses import dataclass
from typing import Optional

from busline.client.subscriber.event_handler import CallbackEventHandler
from busline.event.event import Event
from busline.event.message.number_message import Int64Message
from busline.event.message.string_message import StringMessage
from orbitalis.core.core import Core
from orbitalis.core.requirement import Constraint, OperationRequirement
from orbitalis.core.sink import sink
from orbitalis.core.state import CoreState
from orbitalis.orbiter.schemaspec import Input, Output
from tests.periodic_operations.hello_sender_plugin import HelloSenderPlugin
from tests.periodic_operations.random_number_plugin import RandomNumberPlugin
from tests.utils import build_new_local_client


@dataclass
class MyCore(Core):
    event_received: bool = False

    @sink("randint")
    async def randint_sink(self, topic: str, event: Event[Int64Message]):
        self.event_received = True

    @sink("hello")
    async def hello_sink(self, topic: str, event: Event[Int64Message]):
        self.event_received = True


class TestPlugin(unittest.IsolatedAsyncioTestCase):

    async def test_randint(self):
        plugin = RandomNumberPlugin(
            eventbus_client=build_new_local_client(),
            raise_exceptions=True
        )

        core = MyCore(
            eventbus_client=build_new_local_client(),
            with_loop=False,
            raise_exceptions=True,
            operation_requirements={
                "randint": OperationRequirement(Constraint(
                    inputs=[Input.no_input()],
                    outputs=[Output.from_message(Int64Message)],
                ))
            }
        )

        await plugin.start()
        await core.start()

        await asyncio.sleep(2)      # time for handshake

        self.assertTrue(core.state == CoreState.COMPLIANT)

        await asyncio.sleep(3)      # time to receive random number

        self.assertTrue(core.event_received)

        await plugin.stop()
        await core.stop()

        await asyncio.sleep(1)      # time for close connection

    async def test_hello(self):
        plugin = HelloSenderPlugin(
            eventbus_client=build_new_local_client(),
            raise_exceptions=True
        )

        core = MyCore(
            eventbus_client=build_new_local_client(),
            with_loop=False,
            raise_exceptions=True,
            operation_requirements={
                "hello": OperationRequirement(Constraint(
                    inputs=[Input.no_input()],
                    outputs=[Output.from_message(StringMessage)],
                ))
            }
        )

        await plugin.start()
        await core.start()

        await asyncio.sleep(2)  # time for handshake

        self.assertTrue(core.state == CoreState.COMPLIANT)

        await asyncio.sleep(3)  # time to receive random number

        self.assertTrue(core.event_received)

        await plugin.stop()
        await core.stop()

        await asyncio.sleep(1)  # time for close connection