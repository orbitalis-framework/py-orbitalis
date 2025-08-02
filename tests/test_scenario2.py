import asyncio
import unittest

from orbitalis.core.requirement import Constraint, OperationRequirement
from orbitalis.orbiter.schemaspec import Input, Output
from orbitalis.plugin.operation import Policy
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
            with_loop=False,

            kwh=24      # LampPlugin-specific attribute
        ).with_custom_policy(
            operation_name="turn_on",
            policy=Policy(maximum=2)
        )

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
                "turn_off": OperationRequirement(Constraint(
                    minimum=1,
                    mandatory=[self.lamp_x_plugin.identifier],
                    inputs=[Input.empty()],
                    outputs=[Output.no_output()]
                )),
            }
        )

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


    async def test_handshake(self):
        self.assertFalse(self.smart_home1.is_compliant())

        await self.lamp_x_plugin.start()
        await self.smart_home1.start()

        await asyncio.sleep(2)

        self.assertTrue(self.smart_home1.is_compliant())

        self.assertFalse(self.smart_home2.is_compliant())

        await self.smart_home2.start()

        await asyncio.sleep(2)

        self.assertTrue(self.smart_home2.is_compliant())

        await self.lamp_x_plugin.stop()
        await self.smart_home1.stop()
        await self.smart_home2.stop()

        await asyncio.sleep(2)



