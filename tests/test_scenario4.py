import asyncio
import unittest

from orbitalis.core.requirement import Constraint, OperationRequirement
from orbitalis.core.state import CoreState
from orbitalis.orbiter.schemaspec import Input, Output
from tests.core.smarthome_core import SmartHomeCore
from tests.plugin.lamp.lamp_x_plugin import LampXPlugin
from tests.utils import build_new_local_client


class TestPlugin(unittest.IsolatedAsyncioTestCase):
    """
    Both "smart_home1" "smart_home2" can be compliance.
    """


    def setUp(self):
        self.lamp_x_plugin = LampXPlugin(
            identifier="lamp_x_plugin",
            eventbus_client=build_new_local_client(),
            raise_exceptions=True,
            close_connection_if_unused_after=3,
            pending_requests_expire_after=2,
            loop_interval=0,
            consider_others_dead_after=3,
            send_keepalive_before_timelimit=2,
            graceful_close_timeout=1,

            kwh=1      # LampPlugin-specific attribute
        )

        self.assertTrue("turn_on" in self.lamp_x_plugin.operations)
        self.assertTrue("turn_off" in self.lamp_x_plugin.operations)
        self.assertTrue("get_status" in self.lamp_x_plugin.operations)

        self.smart_home = SmartHomeCore(
            identifier="smart_home",
            eventbus_client=build_new_local_client(),
            discovering_interval=10,
            consider_others_dead_after=3,
            send_keepalive_before_timelimit=2,
            raise_exceptions=True,
            operation_requirements={
                "turn_on": OperationRequirement(
                    Constraint(
                        minimum=1,
                        inputs=[Input.empty()],
                        outputs=[Output.no_output()]
                    )
                ),
                "turn_off": OperationRequirement(
                    Constraint(
                        minimum=1,
                        inputs=[Input.empty()],
                        outputs=[Output.no_output()]
                    )
                ),
            }
        )


    async def test_discard_in_loop(self):
        self.assertFalse(self.smart_home.is_compliant())
        await self.lamp_x_plugin.start()
        await self.smart_home.start()

        await asyncio.sleep(2)  # handshake time

        last_discover = self.smart_home._last_discover_sent_at

        self.assertEqual(len(self.lamp_x_plugin._connections[self.smart_home.identifier]), 2)
        self.assertEqual(len(self.smart_home._connections[self.lamp_x_plugin.identifier]), 2)
        self.assertTrue(self.smart_home.is_compliant())
        self.assertEqual(self.smart_home.state, CoreState.COMPLIANT)

        await asyncio.sleep(3)
        await asyncio.sleep(1)
        await asyncio.sleep(2)

        self.assertEqual(len(self.lamp_x_plugin._connections[self.smart_home.identifier]), 0)
        self.assertEqual(len(self.smart_home._connections[self.lamp_x_plugin.identifier]), 0)
        self.assertFalse(self.smart_home.is_compliant())

        self.lamp_x_plugin.pending_requests_expire_after = None
        self.lamp_x_plugin.close_connection_if_unused_after = None

        await asyncio.sleep(5)      # new discover

        self.assertTrue(self.smart_home._last_discover_sent_at > last_discover)

        await asyncio.sleep(2)      # handshake time

        self.assertEqual(len(self.lamp_x_plugin._connections[self.smart_home.identifier]), 2)
        self.assertEqual(len(self.smart_home._connections[self.lamp_x_plugin.identifier]), 2)
        self.assertEqual(self.smart_home.state, CoreState.COMPLIANT)
        self.assertTrue(self.smart_home.is_compliant())


    async def test_send_keepalive(self):
        await self.lamp_x_plugin.start()
        await self.smart_home.start()

        await asyncio.sleep(2)  # handshake time

        self.assertEqual(self.smart_home.state, CoreState.COMPLIANT)
        self.assertEqual(len(self.smart_home.dead_remote_identifiers), 0)
        self.assertEqual(len(self.lamp_x_plugin.dead_remote_identifiers), 0)

        self.lamp_x_plugin.pause_loop_controller.set()

        await asyncio.sleep(4)

        self.assertEqual(len(self.smart_home.dead_remote_identifiers), 1)

    async def test_graceful_close_timeout(self):
        await self.lamp_x_plugin.start()
        await self.smart_home.start()

        async def dummy(*args, **kwargs):
            pass

        self.smart_home._graceful_close_connection = dummy

        await asyncio.sleep(2)  # handshake time

        self.assertIn(self.smart_home.identifier, self.lamp_x_plugin._connections)
        self.assertIn("turn_on", self.lamp_x_plugin._connections[self.smart_home.identifier])

        await self.lamp_x_plugin.send_graceful_close_connection(
            self.smart_home.identifier,
            "turn_on"
        )

        await asyncio.sleep(2)

        self.assertIn(self.smart_home.identifier, self.lamp_x_plugin._connections)
        self.assertNotIn("turn_on", self.lamp_x_plugin._connections[self.smart_home.identifier])







