import asyncio
import unittest
from typing import Dict

from dataclasses_avroschema import AvroModel

from busline.client.subscriber.topic_subscriber.event_handler import schemafull_event_handler
from busline.local.local_pubsub_client import LocalPubTopicSubClientBuilder
from orbitalis.core.need import ConstrainedNeed
from orbitalis.events.operation_result import OperationResultMessage
from orbitalis.plugin.operation import operation, Policy
from orbitalis.plugin.plugin import Plugin
from busline.event.avro_payload import AvroEventPayload
from busline.event.event import Event
from busline.local.local_pubsub_client import LocalPubTopicSubClientBuilder
from orbitalis.core.core import Core
from dataclasses import dataclass


@dataclass(frozen=True)
class MockOperationMessage(AvroEventPayload):
    value: str


@dataclass
class MockCore(Core):
    async def result_event_handler(self, topic: str, event: Event[OperationResultMessage]):
        pass


@dataclass
class MockPlugin(Plugin):

    operation_call: int = 0
    unlocked: bool = False

    def reset(self):
        self.operation_call = 0
        self.unlocked = False

    @operation(name="operation", schemas=MockOperationMessage.avro_schema_to_python())
    async def mock_operation_event_handler(self, topic: str, event: Event[MockOperationMessage]):
        self.operation_call += 1

        if event.payload.value == "secret":
            self.unlocked = True



class TestPlugin(unittest.IsolatedAsyncioTestCase):


    def setUp(self):
        self.plugin = MockPlugin(
            eventbus_client=LocalPubTopicSubClientBuilder()\
                    .with_default_publisher()\
                    .with_closure_subscriber(lambda t, e: ...)\
                    .build(),
        )

        self.core = MockCore(
            discovering_interval=0.5,
            eventbus_client=LocalPubTopicSubClientBuilder() \
                .with_default_publisher() \
                .with_closure_subscriber(lambda t, e: ...) \
                .build(),
            needed_operations={
                "operation": ConstrainedNeed()
            }
        )

        self.core2 = MockCore(
            eventbus_client=LocalPubTopicSubClientBuilder() \
                .with_default_publisher() \
                .with_closure_subscriber(lambda t, e: ...) \
                .build(),
            needed_operations={
                "operation": ConstrainedNeed()
            }
        )

    async def test_handshake(self):
        self.assertFalse(self.core.is_compliance())

        await self.plugin.start()
        await self.core.start()

        await asyncio.sleep(1)

        self.assertTrue(self.core.is_compliance())

        self.assertFalse(self.core2.is_compliance())

        await self.core2.start()

        self.assertFalse(self.core2.is_compliance())

    async def test_operation(self):
        await self.plugin.start()

        # await self.plugin.eventbus_client.subscribe("operation-topic", self.plugin.mock_operation_event_handler)

        self.plugin.operation_call = 0

        await self.core.eventbus_client.connect()   # without .start() it is needed
        await self.core.sudo_execute(
            "operation-topic",
            MockOperationMessage("abc")     # .into_event() is called within method
        )

        await asyncio.sleep(1)

        self.assertTrue("operation" in self.plugin.operations)
        self.assertEqual(self.plugin.operation_call, 1)
        self.assertFalse(self.plugin.unlocked)

        await self.core.eventbus_client.publish(     # equal to sudo_execute
            "operation-topic",
            MockOperationMessage("secret").into_event()     # need to be an event
        )

        await asyncio.sleep(1)

        self.assertTrue("operation" in self.plugin.operations)
        self.assertEqual(self.plugin.operation_call, 2)
        self.assertTrue(self.plugin.unlocked)




