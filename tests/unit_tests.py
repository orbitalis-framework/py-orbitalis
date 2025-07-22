import asyncio
import unittest

from busline.event.message.avro_message import AvroMessageMixin
from busline.local.local_pubsub_client import LocalPubTopicSubClientBuilder

from busline.local.local_pubsub_client import LocalPubTopicSubClientBuilder
from dataclasses import dataclass

from orbitalis.core.need import Constraint, Need
from orbitalis.orbiter.schemaspec import SchemaSpec, Input, Output
from orbitalis.plugin.operation import Policy
from tests.core.smarthome_core import SmartHomeCore
from tests.plugin.lamp_x_plugin import LampXPlugin
from tests.plugin.lamp_y_plugin import LampYPlugin, TurnOnLampYMessage, TurnOffLampYMessage


@dataclass
class MockMessage(AvroMessageMixin):
    mock: str


class TestPlugin(unittest.IsolatedAsyncioTestCase):


    def setUp(self):
        self.lamp_x_plugin = LampXPlugin(
            identifier="lamp_x_plugin",
            eventbus_client=LocalPubTopicSubClientBuilder.default(),
            raise_exceptions=True,

            kwh=24      # LampPlugin-specific attribute
        ).with_custom_policy(
            operation_name="turn_on",
            policy=Policy.allow_only("smart_home")
        )

        self.assertTrue("turn_on" in self.lamp_x_plugin.operations)
        self.assertTrue("turn_off" in self.lamp_x_plugin.operations)
        self.assertTrue("get_status" in self.lamp_x_plugin.operations)

        self.lamp_y_plugin = LampYPlugin(
            identifier="lamp_y_plugin",
            eventbus_client=LocalPubTopicSubClientBuilder.default(),
            raise_exceptions=True,

            kwh=42
        ).with_custom_policy(
            operation_name="turn_on",
            policy=Policy.no_constraints()      # it is also default
        )

        self.assertTrue("turn_on" in self.lamp_y_plugin.operations)
        self.assertTrue("turn_off" in self.lamp_y_plugin.operations)
        self.assertTrue("get_status" in self.lamp_y_plugin.operations)

        self.smart_home = SmartHomeCore(
            identifier="smart_home",
            eventbus_client=LocalPubTopicSubClientBuilder.default(),
            raise_exceptions=True,
            needed_operations={
                "turn_on": Need(Constraint(
                    minimum=1,
                    mandatory=["lamp_x_plugin"],
                    inputs=[Input.empty()],
                    outputs=[Output.no_output()]
                )),
                "turn_off": Need(Constraint(
                    minimum=1,
                    mandatory=["lamp_x_plugin"],
                    inputs=[Input.empty()],
                    outputs=[Output.no_output()]
                )),
            }
        )

        self.assertTrue("get_status" in self.smart_home.operation_sink)

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
            SchemaSpec(schemas=[MockMessage.avro_schema()], support_empty_schema=True).is_compatible(
                SchemaSpec(schemas=[MockMessage.avro_schema()], support_empty_schema=True)
            )
        )

        self.assertFalse(
            SchemaSpec(schemas=[MockMessage.avro_schema()], support_empty_schema=False).is_compatible(
                SchemaSpec(schemas=[MockMessage.avro_schema()], support_empty_schema=True)
            )
        )


    async def test_turn_on_off_plugin_x(self):
        await self.lamp_x_plugin.start()

        await self.lamp_x_plugin.eventbus_client.subscribe("turn_on_topic", self.lamp_x_plugin.turn_on_event_handler)
        await self.lamp_x_plugin.eventbus_client.subscribe("turn_off_topic", self.lamp_x_plugin.turn_off_event_handler)

        self.lamp_x_plugin.turn_off()
        self.lamp_x_plugin.total_kwh = 0

        await self.smart_home.eventbus_client.connect()   # without .start() it is needed

        await self.smart_home.sudo_execute("turn_on_topic")

        await asyncio.sleep(1)

        self.assertTrue(self.lamp_x_plugin.is_on)

        await self.smart_home.sudo_execute("turn_off_topic")

        await asyncio.sleep(1)

        self.assertTrue(self.lamp_x_plugin.is_off)

        await self.lamp_x_plugin.stop()

    async def test_turn_on_off_plugin_y(self):
        await self.lamp_y_plugin.start()

        await self.lamp_y_plugin.eventbus_client.subscribe("turn_on_topic", self.lamp_y_plugin.turn_on_event_handler)
        await self.lamp_y_plugin.eventbus_client.subscribe("turn_off_topic", self.lamp_y_plugin.turn_off_event_handler)

        self.lamp_y_plugin.turn_off()
        self.lamp_y_plugin.total_kwh = 0

        await self.smart_home.eventbus_client.connect()   # without .start() it is needed

        await self.smart_home.sudo_execute(
            "turn_on_topic",
            TurnOnLampYMessage(power=0.5)    # .into_event() is called within the method
        )

        await asyncio.sleep(1)

        self.assertTrue(self.lamp_y_plugin.is_on)

        await self.smart_home.eventbus_client.publish(
            "turn_off_topic",
            TurnOffLampYMessage(reset_consumption=True).into_event()
        )

        await asyncio.sleep(1)

        self.assertTrue(self.lamp_y_plugin.is_off)

        await self.lamp_y_plugin.stop()


    async def test_keepalive(self):
        self.smart_home.clear_last_seen()

        await self.lamp_x_plugin.start()
        await self.smart_home.start()

        await asyncio.sleep(2)

        self.assertTrue(self.smart_home.is_compliance())

        self.assertTrue(self.lamp_x_plugin.identifier in self.smart_home._last_seen)

        seen1 = self.smart_home._last_seen[self.lamp_x_plugin.identifier]

        await self.lamp_x_plugin.send_keepalive(remote_identifier=self.smart_home.identifier)

        await asyncio.sleep(1)

        seen2 = self.smart_home._last_seen[self.lamp_x_plugin.identifier]

        self.assertGreater(seen2, seen1)

        await self.smart_home.send_keepalive_request(remote_identifier=self.lamp_x_plugin.identifier)

        await asyncio.sleep(1)

        seen3 = self.smart_home._last_seen[self.lamp_x_plugin.identifier]

        self.assertGreater(seen3, seen2)
