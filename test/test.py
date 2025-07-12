import asyncio
import unittest
from typing import Any
from dataclasses_avroschema.pydantic.v1 import AvroBaseModel
from orbitalis.core.need import ConstrainedNeed
from busline.local.local_pubsub_client import LocalPubTopicSubClientBuilder

from busline.event.event import Event
from busline.local.local_pubsub_client import LocalPubTopicSubClientBuilder
from orbitalis.core.core import Core
from dataclasses import dataclass, field

from test.plugin.lamp_x_plugin import LampXPlugin
from test.plugin.lamp_y_plugin import LampYPlugin


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
                "operation": ConstrainedNeed(
                    minimum=1
                )
            }
        )

        self.smart_home2 = SmartHomeCore(
            identifier="smart_home2",
            eventbus_client=LocalPubTopicSubClientBuilder() \
                .with_default_publisher() \
                .with_closure_subscriber(lambda t, e: ...) \
                .build(),
            needed_operations={
                "auth": ConstrainedNeed(
                    minimum=1
                )
            }
        )

    async def test_schema_compatibility(self):
        self.assertTrue(
            self.core.compare_two_schemas(
                MockOperationMessage.avro_schema(),
                MockOperationMessage.avro_schema()
            )
        )

        self.assertTrue(
            self.core.compare_two_schemas(
                "123",
                "123"
            )
        )

        self.assertFalse(
            self.core.compare_two_schemas(
                "123",
                "456"
            )
        )

        self.assertTrue(
            self.core.compare_schema_n_to_n(
                [
                    "123",
                    MockOperationMessage.avro_schema(),
                    "456"
                ],
                [
                    "7654",
                    "9877",
                    MockOperationMessage.avro_schema(),
                    "124"
                ]
            )
        )

        self.assertFalse(
            self.core.compare_schema_n_to_n(
                [
                    "123",
                    "456"
                ],
                [
                    "7654",
                    "9877",
                    MockOperationMessage.avro_schema(),
                    "124"
                ]
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
        await self.core.sudo_execute(
            "dummy-topic",
            MockOperationMessage("abc")     # .into_event() is called within method
        )

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




