import asyncio
import unittest
from dataclasses import dataclass
from typing import Optional

from busline.client.subscriber.event_handler import CallbackEventHandler
from busline.event.event import Event
from busline.event.message.string_message import StringMessage
from orbitalis.core.core import Core
from orbitalis.core.requirement import Constraint, OperationRequirement
from orbitalis.core.sink import sink
from orbitalis.orbiter.schemaspec import Input, Output
from tests.text_processor.lowercase_text_processor_plugin import LowercaseTextProcessorPlugin
from tests.utils import build_new_local_client


@dataclass
class MyCore(Core):
    last_result: Optional[str] = None

    @sink("lowercase")
    async def lowercase_sink(self, topic: str, event: Event[StringMessage]):
        self.last_result = event.payload.value


class TestPlugin(unittest.IsolatedAsyncioTestCase):

   async def test_lowercase_input_output(self):
        plugin = LowercaseTextProcessorPlugin(
            eventbus_client=build_new_local_client(),
            with_loop=False,
            raise_exceptions=True
        )

        core = MyCore(
            eventbus_client=build_new_local_client(),
            with_loop=False,
            raise_exceptions=True,
            operation_requirements={
                "lowercase": OperationRequirement(Constraint(
                    inputs=[Input.from_message(StringMessage)],
                    outputs=[Output.from_message(StringMessage)],
                ), override_sink=CallbackEventHandler(lambda t, e: print(t)), default_setup_data=bytes())
            }
        )

        await plugin.start()
        await core.start()

        await asyncio.sleep(2)

        await core.execute(
            operation_name="lowercase",
            data=StringMessage("HELLO"),
            any=True
        )

        await asyncio.sleep(1)

        self.assertEqual(core.last_result, "hello")

        await plugin.stop()
        await core.stop()

        await asyncio.sleep(1)
