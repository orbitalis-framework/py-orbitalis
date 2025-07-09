import asyncio
import unittest
from typing import Dict

from dataclasses_avroschema import AvroModel

from busline.client.subscriber.topic_subscriber.event_handler import schemafull_event_handler
from busline.local.local_pubsub_client import LocalPubTopicSubClientBuilder
from orbitalis.core.need import ConstrainedNeed
from orbitalis.plugin.operation import operation, Policy
from orbitalis.plugin.plugin import Plugin
from busline.event.avro_payload import AvroEventPayload
from busline.event.event import Event
from busline.local.local_pubsub_client import LocalPubTopicSubClientBuilder
from orbitalis.core.core import Core
from dataclasses import dataclass


@dataclass(frozen=True)
class MockOperationMessage(AvroEventPayload):
    pass


@dataclass
class MockCore(Core):
    pass


@dataclass
class MockPlugin(Plugin):

    operation_call: int = 0

    @schemafull_event_handler(MockOperationMessage.avro_schema_to_python())
    @operation
    async def mock_operation1_event_handler(self, topic: str, event: Event[MockOperationMessage]):
        self.operation_call += 1

    @schemafull_event_handler(MockOperationMessage.avro_schema_to_python())
    @operation(name="operation2", policy=Policy(maximum=1))
    async def mock_operation2_event_handler(self, topic: str, event: Event[MockOperationMessage]):
        self.operation_call += 1



class TestPlugin(unittest.IsolatedAsyncioTestCase):


    def setUp(self):
        self.plugin = MockPlugin(
            eventbus_client=LocalPubTopicSubClientBuilder()\
                    .with_default_publisher()\
                    .with_closure_subscriber(lambda t, e: ...)\
                    .build(),
        )

        self.core = MockCore(
            eventbus_client=LocalPubTopicSubClientBuilder() \
                .with_default_publisher() \
                .with_closure_subscriber(lambda t, e: ...) \
                .build(),
            needed_operations={
                "operation2": ConstrainedNeed()
            }
        )

        self.core2 = MockCore(
            eventbus_client=LocalPubTopicSubClientBuilder() \
                .with_default_publisher() \
                .with_closure_subscriber(lambda t, e: ...) \
                .build(),
            needed_operations={
                "operation2": ConstrainedNeed()
            }
        )

    async def test_handshake(self):
        self.assertFalse(self.core.is_compliance())

        await self.plugin.start()
        await self.core.start()

        await asyncio.sleep(2)

        self.assertTrue(self.core.is_compliance())


        self.assertFalse(self.core2.is_compliance())

        await self.core2.start()

        self.assertFalse(self.core2.is_compliance())





