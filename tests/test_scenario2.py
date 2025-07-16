import asyncio
import unittest
from typing import Any

from busline.event.avro_payload import AvroEventPayload
from busline.local.local_pubsub_client import LocalPubTopicSubClientBuilder

from busline.event.event import Event
from busline.local.local_pubsub_client import LocalPubTopicSubClientBuilder
from orbitalis.core.core import Core
from dataclasses import dataclass, field

from orbitalis.core.need import Constraint, Need
from orbitalis.orbiter.schemaspec import SchemaSpec, Input, Output
from orbitalis.plugin.operation import Policy
from tests.core.smarthome_core import SmartHomeCore
from tests.plugin.lamp_x_plugin import LampXPlugin
from tests.plugin.lamp_y_plugin import LampYPlugin, TurnOnLampYMessage, TurnOffLampYMessage


class TestPlugin(unittest.IsolatedAsyncioTestCase):
    """
    Both "smart_home1" "smart_home2" can be compliance.
    """


    def setUp(self):
        self.lamp_x_plugin = LampXPlugin(
            identifier="lamp_x_plugin",
            eventbus_client=LocalPubTopicSubClientBuilder()\
                    .with_default_publisher()\
                    .with_closure_subscriber(lambda t, e: ...)\
                    .build(),
            raise_exceptions=True,

            kwh=24      # LampPlugin-specific attribute
        ).with_custom_policy(
            operation_name="turn_on",
            policy=Policy(maximum=2)
        )

        self.assertTrue("turn_on" in self.lamp_x_plugin.operations)
        self.assertTrue("turn_off" in self.lamp_x_plugin.operations)
        self.assertTrue("get_status" in self.lamp_x_plugin.operations)

        self.smart_home1 = SmartHomeCore(
            identifier="smart_home1",
            eventbus_client=LocalPubTopicSubClientBuilder() \
                .with_default_publisher() \
                .with_closure_subscriber(lambda t, e: ...) \
                .build(),
            raise_exceptions=True,
            needed_operations={
                "turn_on": Need(Constraint(
                    mandatory=[self.lamp_x_plugin.identifier],
                    inputs=[Input.empty()],
                    outputs=[Output.no_output()]
                )),
                "turn_off": Need(Constraint(
                    mandatory=[self.lamp_x_plugin.identifier],
                    inputs=[Input.empty()],
                    outputs=[Output.no_output()]
                )),
            }
        )

        self.smart_home2 = SmartHomeCore(
            identifier="smart_home2",
            eventbus_client=LocalPubTopicSubClientBuilder() \
                .with_default_publisher() \
                .with_closure_subscriber(lambda t, e: ...) \
                .build(),
            raise_exceptions=True,
            needed_operations={
                "turn_on": Need(Constraint(
                    mandatory=[self.lamp_x_plugin.identifier],
                    inputs=[Input.empty()],
                    outputs=[Output.no_output()]
                )),
                "turn_off": Need(Constraint(
                    mandatory=[self.lamp_x_plugin.identifier],
                    inputs=[Input.empty()],
                    outputs=[Output.no_output()]
                )),
            }
        )


    async def test_handshake(self):
        self.assertFalse(self.smart_home1.is_compliance())

        await self.lamp_x_plugin.start()
        await self.smart_home1.start()

        await asyncio.sleep(2)

        self.assertTrue(self.smart_home1.is_compliance())

        self.assertFalse(self.smart_home2.is_compliance())

        await self.smart_home2.start()

        await asyncio.sleep(2)

        self.assertTrue(self.smart_home2.is_compliance())

        await self.lamp_x_plugin.stop()
        await self.smart_home1.stop()
        await self.smart_home2.stop()

        await asyncio.sleep(2)



