import asyncio
import unittest
from typing import Dict

from dataclasses_avroschema import AvroModel

from busline.local.local_pubsub_client import LocalPubTopicSubClientBuilder
from orbitalis.plugin.configuration import PluginConfiguration
from orbitalis.plugin.plugin import Plugin
from orbitalis.utils.policy import Policy
from busline.event.avro_payload import AvroEventPayload
from busline.event.event import Event
from busline.local.local_pubsub_client import LocalPubTopicSubClientBuilder
from orbitalis.core.core import Core
from orbitalis.core.core_service import CoreService, CoreServiceDescriptor, CoreServiceNeed
from orbitalis.utils.policy import Policy
from orbitalis.utils.service import Feature
from dataclasses import dataclass


@dataclass(frozen=True)
class MockMessage(AvroEventPayload):
    pass


@dataclass
class MockFeature(Feature):

    def input_schema(self) -> Dict:
        return AvroModel.avro_schema_to_python(MockMessage)

    async def handle(self, topic: str, event: Event[MockMessage]):
        pass


@dataclass
class MockCore(Core):
    pass


class MockPlugin(Plugin):
    pass


class TestPlugin(unittest.IsolatedAsyncioTestCase):


    def setUp(self):
        self.plugin = MockPlugin(
            identifier="plugin-id",
            eventbus_client=LocalPubTopicSubClientBuilder()\
                    .with_default_publisher()\
                    .with_closure_subscriber(lambda t, e: ...)\
                    .build(),
        )

        self.core = MockCore(
            services={
                "test-service": CoreServiceDescriptor(
                    service=CoreService(
                        description="test service",
                        features={
                            "test-feature1": MockFeature()
                        }
                    ),
                    mandatory=True,
                    need=CoreServiceNeed(
                        mandatory_plugins_by_identifier={ "plugin-id" }
                    )
                )
            },
            eventbus_client=LocalPubTopicSubClientBuilder() \
                .with_default_publisher() \
                .with_closure_subscriber(lambda t, e: ...) \
                .build(),
        )

    async def test_core_compliance(self):
        self.assertFalse(self.core.is_compliance())

        self.core.plug("test-service", self.plugin.generate_descriptor())

        self.assertTrue(self.core.is_compliance())

    async def test_handshake(self):
        self.assertFalse(self.core.is_compliance())
        await self.plugin.start()
        await self.core.start()

        await self.core.send_discover()

        await asyncio.sleep(2)
        self.assertTrue(self.core.is_compliance())





