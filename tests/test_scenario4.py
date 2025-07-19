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
from orbitalis.core.state import CoreState
from orbitalis.orbiter.schemaspec import SchemaSpec, Input, Output
from orbitalis.plugin.operation import Policy
from tests.core.smarthome_core import SmartHomeCore
from tests.plugin.lamp_x_plugin import LampXPlugin
from tests.plugin.lamp_y_plugin import LampYPlugin, TurnOnLampYMessage, TurnOffLampYMessage


class TestPlugin(unittest.IsolatedAsyncioTestCase):
    """
    Both "smart_home1" "smart_home2" can be compliance.
    """


    def setUp(self):
        self.lamp_x_plugin = LampXPlugin(
            identifier="lamp_x_plugin",
            eventbus_client=LocalPubTopicSubClientBuilder.default(),
            raise_exceptions=True,
            discard_unused_connections_interval=1,
            discard_pending_requests_loop_interval=1,
            discard_connection_if_unused_after=4,
            pending_request_expires_after=1,
            send_keepalive_interval=1,

            kwh=1      # LampPlugin-specific attribute
        )

        self.assertTrue("turn_on" in self.lamp_x_plugin.operations)
        self.assertTrue("turn_off" in self.lamp_x_plugin.operations)
        self.assertTrue("get_status" in self.lamp_x_plugin.operations)

        self.smart_home = SmartHomeCore(
            identifier="smart_home",
            eventbus_client=LocalPubTopicSubClientBuilder.default(),
            raise_exceptions=True,
            needed_operations={
                "turn_on": Need(
                    Constraint(
                        inputs=[Input.empty()],
                        outputs=[Output.no_output()]
                    )
                ),
                "turn_off": Need(
                    Constraint(
                        inputs=[Input.empty()],
                        outputs=[Output.no_output()]
                    )
                ),
            }
        )

    async def test_loops(self):
        await self.lamp_x_plugin.start()
        await self.smart_home.start()

        await asyncio.sleep(2)

        self.assertEqual(self.smart_home.state, CoreState.COMPLIANT)

        await asyncio.sleep(4)

        self.assertFalse(self.smart_home.is_compliance())




