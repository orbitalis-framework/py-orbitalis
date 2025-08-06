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
from orbitalis.plugin.operation import operation
from orbitalis.plugin.plugin import Plugin
from tests.periodic_operations.hello_sender_plugin import HelloSenderPlugin
from tests.periodic_operations.random_number_plugin import RandomNumberPlugin
from tests.utils import build_new_local_client


@dataclass
class SaveIntegerPlugin(Plugin):
    """
    Save inbound integer
    """

    vault: Optional[int] = None

    @operation(
        name="save",  # operation's name
        input=Input.int64(),  # operation's input, i.e. an int number
        output=Output.no_output()  # operation's output, i.e. no outputs
        # no policy is specified => Policy.no_constraints()
    )
    async def save_int_event_handler(self, topic: str, event: Event[Int64Message]):
        self.vault = event.payload.value


@dataclass
class SaveStringPlugin(Plugin):
    """
    Save inbound string
    """

    vault: Optional[str] = None

    @operation(
        name="save",  # operation's name
        input=Input.string(),  # operation's input, i.e. a string
        output=Output.no_output()  # operation's output, i.e. no outputs
        # no policy is specified => Policy.no_constraints()
    )
    async def save_int_event_handler(self, topic: str, event: Event[StringMessage]):
        self.vault = event.payload.value



@dataclass
class MyCore(Core):

    async def execute_dynamically(self):
        """
        Send right data type based on plugin operations
        """

        # First retrieve all connections related to operation
        connections = self._retrieve_connections(operation_name="save")

        for connection in connections:
            # Ignore connection without an input
            if not connection.has_input:
                continue

            # If connection input has a schema compatible with Int64Message send 42
            if connection.input.is_compatible_with_schema(Int64Message.avro_schema()):
                await self.eventbus_client.publish(
                    connection.input_topic,
                    42
                )

            # If connection input has a schema compatible with StringMessage send "hello"
            if connection.input.is_compatible_with_schema(StringMessage.avro_schema()):
                await self.eventbus_client.publish(
                    connection.input_topic,
                    "hello"
                )


class TestPlugin(unittest.IsolatedAsyncioTestCase):

    async def test_dynamic_input(self):
        int_plugin = SaveIntegerPlugin(
            identifier="int_plugin",
            eventbus_client=build_new_local_client(),
            raise_exceptions=True,
            with_loop=False,
        )

        str_plugin = SaveStringPlugin(
            identifier="str_plugin",
            eventbus_client=build_new_local_client(),
            raise_exceptions=True,
            with_loop=False,
        )

        core = MyCore(
            eventbus_client=build_new_local_client(),
            with_loop=False,
            raise_exceptions=True,
            operation_requirements={
                "save": OperationRequirement(Constraint(
                    inputs=[Input.int64(), Input.string()],
                    outputs=[Output.no_output()],
                    mandatory=["int_plugin", "str_plugin"]
                ))
            }
        )

        await int_plugin.start()
        await str_plugin.start()
        await core.start()

        await asyncio.sleep(2)      # time for handshake

        self.assertTrue(core.state == CoreState.COMPLIANT)

        await core.execute_dynamically()

        await asyncio.sleep(2)  # time to execute

        self.assertEqual(int_plugin.vault, 42)
        self.assertEqual(str_plugin.vault, "hello")

        await int_plugin.stop()
        await str_plugin.stop()
        await core.stop()

        await asyncio.sleep(1)      # time for close connection
