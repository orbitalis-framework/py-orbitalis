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
from tests.plugin.lamp_plugin import StatusMessage
from tests.plugin.lamp_x_plugin import LampXPlugin
from tests.plugin.lamp_y_plugin import LampYPlugin, TurnOnLampYMessage, TurnOffLampYMessage


class TestPlugin(unittest.IsolatedAsyncioTestCase):
    """
    "smart_home1" can be compliance, "smart_home2" no due to "turn_on" operation of "lamp_x_plugin" which can serve only
    "smart_home1".
    """


    def setUp(self):
        self.lamp_x_plugin = LampXPlugin(
            identifier="lamp_x_plugin",
            eventbus_client=LocalPubTopicSubClientBuilder.default(),
            raise_exceptions=True,

            kwh=24      # LampPlugin-specific attribute
        ).with_custom_policy(
            operation_name="turn_on",
            policy=Policy(allowlist=["smart_home1"])
        )

        self.lamp_x_plugin.eventbus_client.subscribers[0].identifier = "lamp_x_plugin_subscriber"

        self.assertTrue("turn_on" in self.lamp_x_plugin.operations)
        self.assertTrue("turn_off" in self.lamp_x_plugin.operations)
        self.assertTrue("get_status" in self.lamp_x_plugin.operations)

        self.smart_home1 = SmartHomeCore(
            identifier="smart_home1",
            eventbus_client=LocalPubTopicSubClientBuilder.default(),
            raise_exceptions=True,
            needed_operations={
                "turn_on": Need(Constraint(
                    mandatory=[self.lamp_x_plugin.identifier],
                    inputs=[Input.empty()],
                    outputs=[Output.no_output()]
                )),
                "turn_off": Need(
                    Constraint(
                        mandatory=[self.lamp_x_plugin.identifier],
                        inputs=[Input.empty()],
                        outputs=[Output.no_output()]
                    )
                ),
                "get_status": Need(
                    Constraint(
                        inputs=[Input.empty()],
                        outputs=[Output.from_schema(StatusMessage.avro_schema())]
                    )
                )
            }
        )

        self.smart_home1.eventbus_client.subscribers[0].identifier = "smart_home1_subscriber"

        self.smart_home2 = SmartHomeCore(
            identifier="smart_home2",
            eventbus_client=LocalPubTopicSubClientBuilder.default(),
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

    async def test_simple_handshake(self):
        self.assertFalse(self.smart_home1.is_compliance())

        await self.lamp_x_plugin.start()
        await self.smart_home1.start()

        await asyncio.sleep(2)

        self.assertTrue(self.smart_home1.is_compliance())

    async def test_double_handshake(self):
        await self.test_simple_handshake()

        self.assertFalse(self.smart_home2.is_compliance())

        await self.smart_home2.start()

        await asyncio.sleep(2)

        self.assertFalse(self.smart_home2.is_compliance())

    async def test_core_plugin_life(self):

        # === HANDSHAKE ===
        self.assertFalse(self.smart_home1.is_compliance())

        await self.lamp_x_plugin.start()
        await self.smart_home1.start()

        await asyncio.sleep(1)

        self.assertTrue(self.smart_home1.is_compliance())

        self.assertTrue(len(self.smart_home1.retrieve_connections(
            remote_identifier=self.lamp_x_plugin.identifier,
            operation_name="turn_on"
        )) == 1)

        self.assertTrue(len(self.lamp_x_plugin.retrieve_connections(
            remote_identifier=self.smart_home1.identifier,
            operation_name="turn_on"
        )) == 1)

        self.assertTrue(len(self.lamp_x_plugin.retrieve_connections(
            remote_identifier=self.smart_home1.identifier,
            operation_name="turn_off"
        )) == 1)

        # === TEST "turn_on" ===

        self.lamp_x_plugin.turn_off()
        self.assertTrue(self.lamp_x_plugin.is_off)

        await self.smart_home1.execute("turn_on", plugin_identifier=self.lamp_x_plugin.identifier)

        await asyncio.sleep(1)

        self.assertTrue(self.lamp_x_plugin.is_on)


        # === CLOSE GRACELESS ===

        await self.smart_home1.graceless_close_connection(self.lamp_x_plugin.identifier, "turn_on")

        await asyncio.sleep(1)

        self.assertTrue(len(self.smart_home1.retrieve_connections(
            remote_identifier=self.lamp_x_plugin.identifier,
            operation_name="turn_on"
        )) == 0)

        self.assertFalse(self.smart_home1.is_compliance())

        await self.lamp_x_plugin.graceless_close_connection(self.smart_home1.identifier, "turn_off")

        await asyncio.sleep(1)

        self.assertTrue(len(self.smart_home1.retrieve_connections(
            remote_identifier=self.lamp_x_plugin.identifier,
            operation_name="turn_off"
        )) == 0)

        self.assertFalse(self.smart_home1.is_compliance())

        # === TEST "turn_on" (should not work) ===

        self.lamp_x_plugin.turn_off()
        self.assertTrue(self.lamp_x_plugin.is_off)

        await self.smart_home1.execute("turn_on", plugin_identifier=self.lamp_x_plugin.identifier)

        await asyncio.sleep(1)

        self.assertTrue(self.lamp_x_plugin.is_off)

        # === RECONNECT ===

        await self.smart_home1.send_discover_based_on_needs()

        await asyncio.sleep(1)

        self.assertTrue(self.smart_home1.is_compliance())

        # === CLOSE GRACEFUL ===

        await self.lamp_x_plugin.graceful_close_connection(
            self.smart_home1.identifier,
            "turn_on"
        )

        await asyncio.sleep(1)

        self.assertFalse(self.smart_home1.is_compliance())


    async def test_get_status(self):
        self.assertFalse(self.smart_home1.is_compliance())

        await self.lamp_x_plugin.start()
        await self.smart_home1.start()

        await asyncio.sleep(1)

        await self.smart_home1.execute("get_status", plugin_identifier=self.lamp_x_plugin.identifier)

        await asyncio.sleep(1)

        self.assertTrue(self.lamp_x_plugin.identifier in self.smart_home1.lamp_status)


