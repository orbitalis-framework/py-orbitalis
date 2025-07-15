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
from orbitalis.orbiter.schemaspec import SchemaSpec
from orbitalis.plugin.operation import Policy
from tests.core.smarthome_core import SmartHomeCore
from tests.plugin.lamp_x_plugin import LampXPlugin
from tests.plugin.lamp_y_plugin import LampYPlugin, TurnOnMessage, TurnOffMessage


@dataclass
class MockMessage(AvroEventPayload):
    mock: str


class TestPlugin(unittest.IsolatedAsyncioTestCase):


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
            policy=Policy.allow_only("smart_home1")
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
        ).with_custom_policy(
            operation_name="turn_on",
            policy=Policy.no_constraints()      # it is also default
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
                "turn_on": Need(Constraint(
                    mandatory=["lamp_x_plugin"],
                    input=SchemaSpec.empty()
                )),
                "turn_off": Need(Constraint(
                    mandatory=["lamp_x_plugin"],
                    input=SchemaSpec.empty()
                )),
            }
        )

        self.assertTrue("get_status" in self.smart_home1.operation_sink)

    async def test_schemaspec_compatibility(self):
        self.assertTrue(
            SchemaSpec.empty().is_compatible(
                SchemaSpec.empty()
            )
        )

        self.assertFalse(
            SchemaSpec.empty().is_compatible(
                SchemaSpec.from_schema(MockMessage.avro_schema())
            )
        )

        self.assertFalse(
            SchemaSpec.from_schema(MockMessage.avro_schema()).is_compatible(
                SchemaSpec.empty()
            )
        )

        self.assertTrue(
            SchemaSpec.from_schema(MockMessage.avro_schema()).is_compatible(
                SchemaSpec.from_payload(MockMessage)
            )
        )

        self.assertTrue(
            SchemaSpec(schemas=[MockMessage.avro_schema()], empty_schema=True).is_compatible(
                SchemaSpec(schemas=[MockMessage.avro_schema()], empty_schema=True)
            )
        )

        self.assertFalse(
            SchemaSpec(schemas=[MockMessage.avro_schema()], empty_schema=False).is_compatible(
                SchemaSpec(schemas=[MockMessage.avro_schema()], empty_schema=True)
            )
        )


    async def test_turn_on_off_plugin_x(self):
        await self.lamp_x_plugin.start()

        await self.lamp_x_plugin.eventbus_client.subscribe("turn_on_topic", self.lamp_x_plugin.turn_on_event_handler)
        await self.lamp_x_plugin.eventbus_client.subscribe("turn_off_topic", self.lamp_x_plugin.turn_off_event_handler)

        self.lamp_x_plugin.turn_off()
        self.lamp_x_plugin.total_kwh = 0

        await self.smart_home1.eventbus_client.connect()   # without .start() it is needed

        await self.smart_home1.sudo_execute("turn_on_topic")

        await asyncio.sleep(1)

        self.assertTrue(self.lamp_x_plugin.is_on)

        await self.smart_home1.sudo_execute("turn_off_topic")

        await asyncio.sleep(1)

        self.assertTrue(self.lamp_x_plugin.is_off)

        await self.lamp_x_plugin.stop()

    async def test_turn_on_off_plugin_y(self):
        await self.lamp_y_plugin.start()

        await self.lamp_y_plugin.eventbus_client.subscribe("turn_on_topic", self.lamp_y_plugin.turn_on_event_handler)
        await self.lamp_y_plugin.eventbus_client.subscribe("turn_off_topic", self.lamp_y_plugin.turn_off_event_handler)

        self.lamp_y_plugin.turn_off()
        self.lamp_y_plugin.total_kwh = 0

        await self.smart_home1.eventbus_client.connect()   # without .start() it is needed

        await self.smart_home1.sudo_execute(
            "turn_on_topic",
            TurnOnMessage(power=0.5)    # .into_event() is called within the method
        )

        await asyncio.sleep(1)

        self.assertTrue(self.lamp_y_plugin.is_on)

        await self.smart_home1.eventbus_client.publish(
            "turn_off_topic",
            TurnOffMessage(reset_consumption=True).into_event()
        )

        await asyncio.sleep(1)

        self.assertTrue(self.lamp_y_plugin.is_off)

        await self.lamp_y_plugin.stop()




