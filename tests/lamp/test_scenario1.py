import asyncio
import unittest

from orbitalis.core.requirement import Constraint, OperationRequirement
from orbitalis.orbiter.schemaspec import Input, Output
from orbitalis.plugin.operation import Policy
from tests.lamp.smarthome_core import SmartHomeCore
from tests.lamp.plugin.lamp_plugin import StatusMessage
from tests.lamp.plugin.lamp_x_plugin import LampXPlugin
from tests.utils import build_new_local_client


class TestPlugin(unittest.IsolatedAsyncioTestCase):
    """
    "smart_home1" can be compliance, "smart_home2" no due to "turn_on" operation of "lamp_x_plugin" which can serve only
    "smart_home1".
    """

    def setUp(self):
        self.lamp_x_plugin = LampXPlugin(
            identifier="lamp_x_plugin",
            eventbus_client=build_new_local_client(),
            raise_exceptions=True,
            with_loop=False,

            kw=24      # LampPlugin-specific attribute
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
            eventbus_client=build_new_local_client(),
            raise_exceptions=True,
            with_loop=False,
            operation_requirements={
                "turn_on": OperationRequirement(Constraint(
                    minimum=1,
                    mandatory=[self.lamp_x_plugin.identifier],
                    inputs=[Input.empty()],
                    outputs=[Output.no_output()]
                )),
                "turn_off": OperationRequirement(
                    Constraint(
                        minimum=1,
                        mandatory=[self.lamp_x_plugin.identifier],
                        inputs=[Input.empty()],
                        outputs=[Output.no_output()]
                    )
                ),
                "get_status": OperationRequirement(
                    Constraint(
                        minimum=1,
                        inputs=[Input.empty()],
                        outputs=[Output.from_schema(StatusMessage.avro_schema())]
                    )
                )
            }
        )

        self.smart_home1.eventbus_client.subscribers[0].identifier = "smart_home1_subscriber"

        self.smart_home2 = SmartHomeCore(
            identifier="smart_home2",
            eventbus_client=build_new_local_client(),
            raise_exceptions=True,
            with_loop=False,
            operation_requirements={
                "turn_on": OperationRequirement(Constraint(
                    minimum=1,
                    mandatory=[self.lamp_x_plugin.identifier],
                    inputs=[Input.empty()],
                    outputs=[Output.no_output()]
                )),
                "turn_off": OperationRequirement(Constraint(
                    minimum=1,
                    mandatory=[self.lamp_x_plugin.identifier],
                    inputs=[Input.empty()],
                    outputs=[Output.no_output()]
                )),
            }
        )

    async def test_simple_handshake(self):
        self.assertFalse(self.smart_home1.is_compliant())

        await self.lamp_x_plugin.start()
        await self.smart_home1.start()

        await asyncio.sleep(2)

        self.assertTrue(self.smart_home1.is_compliant())

        await asyncio.gather(
            self.lamp_x_plugin.stop(),
            self.smart_home1.stop()
        )

        await asyncio.sleep(2)

    async def test_double_handshake(self):
        self.assertFalse(self.smart_home1.is_compliant())
        self.assertFalse(self.smart_home2.is_compliant())

        await self.lamp_x_plugin.start()
        await self.smart_home1.start()

        await asyncio.sleep(2)

        await self.smart_home2.start()

        await asyncio.sleep(2)

        self.assertFalse(self.smart_home2.is_compliant())

        await asyncio.gather(
            self.lamp_x_plugin.stop(),
            self.smart_home1.stop(),
            self.smart_home2.stop(),
        )

        await asyncio.sleep(2)

    async def test_core_plugin_life(self):

        # === HANDSHAKE ===
        self.assertFalse(self.smart_home1.is_compliant())

        await self.lamp_x_plugin.start()
        await self.smart_home1.start()

        await asyncio.sleep(1)

        self.assertTrue(self.smart_home1.is_compliant())

        self.assertTrue(len(self.smart_home1._retrieve_connections(
            remote_identifier=self.lamp_x_plugin.identifier,
            operation_name="turn_on"
        )) == 1)

        self.assertTrue(len(self.lamp_x_plugin._retrieve_connections(
            remote_identifier=self.smart_home1.identifier,
            operation_name="turn_on"
        )) == 1)

        self.assertTrue(len(self.lamp_x_plugin._retrieve_connections(
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

        await self.smart_home1.send_graceless_close_connection(self.lamp_x_plugin.identifier, "turn_on")

        await asyncio.sleep(1)

        self.assertTrue(len(self.smart_home1._retrieve_connections(
            remote_identifier=self.lamp_x_plugin.identifier,
            operation_name="turn_on"
        )) == 0)

        self.assertFalse(self.smart_home1.is_compliant())

        await self.lamp_x_plugin.send_graceless_close_connection(self.smart_home1.identifier, "turn_off")

        await asyncio.sleep(1)

        self.assertTrue(len(self.smart_home1._retrieve_connections(
            remote_identifier=self.lamp_x_plugin.identifier,
            operation_name="turn_off"
        )) == 0)

        self.assertFalse(self.smart_home1.is_compliant())

        # === TEST "turn_on" (should not work) ===

        self.lamp_x_plugin.turn_off()
        self.assertTrue(self.lamp_x_plugin.is_off)

        await self.smart_home1.execute("turn_on", plugin_identifier=self.lamp_x_plugin.identifier)

        await asyncio.sleep(1)

        self.assertTrue(self.lamp_x_plugin.is_off)

        # === RECONNECT ===

        await self.smart_home1.send_discover_based_on_requirements()

        await asyncio.sleep(1)

        self.assertTrue(self.smart_home1.is_compliant())

        # === CLOSE GRACEFUL ===

        await self.lamp_x_plugin.send_graceful_close_connection(
            self.smart_home1.identifier,
            "turn_on"
        )

        await asyncio.sleep(1)

        self.assertFalse(self.smart_home1.is_compliant())

        await asyncio.gather(
            self.lamp_x_plugin.stop(),
            self.smart_home1.stop(),
        )

        await asyncio.sleep(2)


    async def test_get_status(self):
        self.assertFalse(self.smart_home1.is_compliant())

        await self.lamp_x_plugin.start()
        await self.smart_home1.start()

        await asyncio.sleep(1)

        await self.smart_home1.execute("get_status", plugin_identifier=self.lamp_x_plugin.identifier)

        await asyncio.sleep(1)

        self.assertTrue(self.lamp_x_plugin.identifier in self.smart_home1.lamp_status)

        await asyncio.gather(
            self.lamp_x_plugin.stop(),
            self.smart_home1.stop()
        )

        await asyncio.sleep(2)

    async def test_execute_without_handshake(self):
        await self.smart_home1.execute("get_status", plugin_identifier=self.lamp_x_plugin.identifier)

        await asyncio.sleep(1)

        self.assertTrue(self.lamp_x_plugin.identifier not in self.smart_home1.lamp_status)


