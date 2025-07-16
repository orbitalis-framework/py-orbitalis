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
from orbitalis.orbiter.schemaspec import SchemaSpec, Input
from orbitalis.plugin.operation import Policy
from tests.core.smarthome_core import SmartHomeCore
from tests.plugin.lamp_x_plugin import LampXPlugin
from tests.plugin.lamp_y_plugin import LampYPlugin, TurnOnMessage, TurnOffMessage


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
        )

        self.assertTrue("turn_on" in self.lamp_x_plugin.operations)
        self.assertTrue("turn_off" in self.lamp_x_plugin.operations)
        self.assertTrue("get_status" in self.lamp_x_plugin.operations)

        self.lamp_y_plugin = LampYPlugin(
            identifier="lamp_y_plugin",
            eventbus_client=LocalPubTopicSubClientBuilder() \
                .with_default_publisher() \
                .with_closure_subscriber(lambda t, e: ...) \
                .build(),
            raise_exceptions=True,

            kwh=42
        )

        self.assertTrue("turn_on" in self.lamp_y_plugin.operations)
        self.assertTrue("turn_off" in self.lamp_y_plugin.operations)
        self.assertTrue("get_status" in self.lamp_y_plugin.operations)

        self.smart_home1 = SmartHomeCore(
            identifier="smart_home1",
            eventbus_client=LocalPubTopicSubClientBuilder() \
                .with_default_publisher() \
                .with_closure_subscriber(lambda t, e: ...) \
                .build(),
            raise_exceptions=True,
            needed_operations={
                "turn_on": Need(Constraint().with_input(Input.from_schema(TurnOnMessage.avro_schema()).with_empty_support())),
                "turn_off": Need(Constraint().with_input(Input.from_schema(TurnOnMessage.avro_schema()).with_empty_support())),
            }
        )


    async def test_all_any_execute(self):
        self.assertTrue(self.smart_home1.is_compliance())   # already compliance, but can plug plugins

        await self.lamp_x_plugin.start()
        await self.lamp_y_plugin.start()
        await self.smart_home1.start()

        await asyncio.sleep(2)

        self.assertTrue(self.lamp_x_plugin.identifier in self.smart_home1.connections.keys())
        self.assertTrue("turn_on" in self.smart_home1.connections[self.lamp_x_plugin.identifier].keys())
        self.assertTrue("turn_off" in self.smart_home1.connections[self.lamp_x_plugin.identifier].keys())
        self.assertTrue(self.smart_home1.is_compliance(), "Core 'smart_home1' not compliance")

        self.lamp_x_plugin.turn_off()
        self.lamp_y_plugin.turn_off()

        await self.smart_home1.execute("turn_on", all=True)

        await asyncio.sleep(2)

        self.assertTrue(self.lamp_x_plugin.is_on, "'lamp_x_plugin' is off, but it should be turned on")
        self.assertFalse(self.lamp_y_plugin.is_off, "'lamp_y_plugin' is on, but payload was incompatible")     # because no payload was used during execute, which is compatible



