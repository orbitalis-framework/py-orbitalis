import asyncio
import unittest
from typing import Any
from dataclasses_avroschema.pydantic.v1 import AvroBaseModel

from busline.event.avro_payload import AvroEventPayload
from busline.local.local_pubsub_client import LocalPubTopicSubClientBuilder

from busline.event.event import Event
from busline.local.local_pubsub_client import LocalPubTopicSubClientBuilder
from orbitalis.core.core import Core
from dataclasses import dataclass, field

from orbitalis.orbiter.schemaspec import SchemaSpec
from test.plugin.lamp_x_plugin import LampXPlugin
from test.plugin.lamp_y_plugin import LampYPlugin


@dataclass
class MockMessage(AvroEventPayload):
    mock: str


@dataclass
class SmartHomeCore(Core):
    pass



class TestPlugin(unittest.IsolatedAsyncioTestCase):


    def setUp(self):
        self.lamp_x_plugin = LampXPlugin(
            identifier="lamp_x_plugin",
            eventbus_client=LocalPubTopicSubClientBuilder()\
                    .with_default_publisher()\
                    .with_closure_subscriber(lambda t, e: ...)\
                    .build(),
            kwh=24
        )

        self.assertTrue("turn_on" in self.lamp_x_plugin.operations)
        self.assertTrue("turn_off" in self.lamp_x_plugin.operations)
        self.assertTrue("get_status" in self.lamp_x_plugin.operations)

        self.lamp_y_plugin = LampYPlugin(
            identifier="lamp_x_plugin",
            eventbus_client=LocalPubTopicSubClientBuilder() \
                .with_default_publisher() \
                .with_closure_subscriber(lambda t, e: ...) \
                .build(),
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
            needed_operations={
            }
        )

        self.smart_home2 = SmartHomeCore(
            identifier="smart_home2",
            eventbus_client=LocalPubTopicSubClientBuilder() \
                .with_default_publisher() \
                .with_closure_subscriber(lambda t, e: ...) \
                .build(),
            needed_operations={
            }
        )

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

    async def test_handshake(self):
        self.assertFalse(self.core.is_compliance())

        await self.plugin.start()
        await self.core.start()

        await asyncio.sleep(1)

        self.assertTrue(self.core.is_compliance())

        self.assertFalse(self.core2.is_compliance())

        await self.core2.start()

        await asyncio.sleep(1)

        self.assertFalse(self.core2.is_compliance())


    async def test_operation(self):
        await self.plugin.start()

        await self.plugin.eventbus_client.subscribe("dummy-topic", self.plugin.dummy_event_handler)

        self.plugin.operation_call = 0

        await self.core.eventbus_client.connect()   # without .start() it is needed
        # await self.core.sudo_execute(
        #     "dummy-topic",
        #     MockOperationMessage("abc")     # .into_event() is called within method
        # )

        await asyncio.sleep(1)

        self.assertEqual(self.plugin.operation_call, 1)
        self.assertEqual(self.plugin.last_value, "abc")

        await self.core.eventbus_client.publish(     # equal to sudo_execute
            "dummy-topic",
            MockOperationMessage("secret").into_event()     # need to be an event
        )

        await asyncio.sleep(1)

        self.assertEqual(self.plugin.operation_call, 2)
        self.assertEqual(self.plugin.last_value, "secret")




