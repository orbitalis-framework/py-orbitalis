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

    async def auth_event_handler(self, topic: str, event: Event[OperationResultMessage]):
        pass


@dataclass
class AuthPlugin(Plugin):

    operation_call: int = 0
    unlocked: bool = False
    last_value: str = ""

    def reset(self):
        self.operation_call = 0
        self.unlocked = False

    @operation(
        name="auth",
        input_schemas=MockOperationMessage.avro_schema_to_python(),
        has_output=True,
        policy=Policy(allowlist=["core1"])
    )
    async def auth_event_handler(self, topic: str, event: Event[MockOperationMessage]):
        self.operation_call += 1
        self.last_value = event.payload.value

        if event.payload.value == "secret":
            outcome = (1).to_bytes(1)
            self.unlocked = True
        else:
            outcome = (0).to_bytes(1)
            self.unlocked = False

        connections = self.from_input_topic_to_connections(topic, "auth")

        assert len(connections) == 1
        connection = connections[0]

        await self.send_result(
            connection.output_topic,
            "auth",
            data=outcome
        )

    @operation(
        name="dummy",
        input_schemas=MockOperationMessage.avro_schema_to_python(),
        has_output=True,
        policy=Policy(maximum=1)
    )
    async def dummy_event_handler(self, topic: str, event: Event[MockOperationMessage]):
        self.operation_call += 1
        self.last_value = event.payload.value



class TestPlugin(unittest.IsolatedAsyncioTestCase):


    def setUp(self):
        self.plugin = AuthPlugin(
            identifier="plugin",
            eventbus_client=LocalPubTopicSubClientBuilder()\
                    .with_default_publisher()\
                    .with_closure_subscriber(lambda t, e: ...)\
                    .build(),
        )

        self.assertTrue("auth" in self.plugin.operations.keys())
        self.assertTrue("dummy" in self.plugin.operations)

        self.core = MockCore(
            identifier="core",
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

        self.core2 = MockCore(
            identifier="core2",
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




