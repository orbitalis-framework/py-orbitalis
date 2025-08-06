import asyncio
import unittest

from orbitalis.core.requirement import Constraint, OperationRequirement
from orbitalis.orbiter.schemaspec import Input, Output
from tests.lamp.smarthome_core import SmartHomeCore
from tests.lamp.plugin.lamp_x_plugin import LampXPlugin
from tests.lamp.plugin.lamp_y_plugin import LampYPlugin, TurnOnLampYMessage, TurnOffLampYMessage
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

            kw=24      # LampPlugin-specific attribute
        )

        self.assertTrue("turn_on" in self.lamp_x_plugin.operations)
        self.assertTrue("turn_off" in self.lamp_x_plugin.operations)
        self.assertTrue("get_status" in self.lamp_x_plugin.operations)

        self.lamp_y_plugin = LampYPlugin(
            identifier="lamp_y_plugin",
            eventbus_client=build_new_local_client(),
            raise_exceptions=True,
            with_loop=False,

            kw=42
        )

        self.assertTrue("turn_on" in self.lamp_y_plugin.operations)
        self.assertTrue("turn_off" in self.lamp_y_plugin.operations)
        self.assertTrue("get_status" in self.lamp_y_plugin.operations)

        self.smart_home1 = SmartHomeCore(
            identifier="smart_home1",
            eventbus_client=build_new_local_client(),
            raise_exceptions=True,
            with_loop=False,
            operation_requirements={
                "turn_on": OperationRequirement(
                    Constraint(
                        minimum=0,
                        inputs=[Input.empty(), Input.from_schema(TurnOnLampYMessage.avro_schema())],
                        outputs=[Output.no_output()]
                    )
                ),
                "turn_off": OperationRequirement(
                    Constraint(
                        minimum=0,
                        inputs=[Input.empty(), Input.from_schema(TurnOffLampYMessage.avro_schema())],
                        outputs=[Output.no_output()]
                    )
                ),
            }
        )


    async def test_all_any_execute(self):
        self.assertTrue(self.smart_home1.is_compliant())   # already compliance, but can plug plugins

        await self.lamp_x_plugin.start()
        await self.lamp_y_plugin.start()
        await self.smart_home1.start()

        await asyncio.sleep(2)

        self.assertTrue(self.lamp_x_plugin.identifier in self.smart_home1._connections.keys())
        self.assertTrue("turn_on" in self.smart_home1._connections[self.lamp_x_plugin.identifier].keys())
        self.assertTrue("turn_off" in self.smart_home1._connections[self.lamp_x_plugin.identifier].keys())
        self.assertTrue(self.smart_home1.is_compliant(), "Core 'smart_home1' not compliance")

        self.lamp_x_plugin.turn_off()
        self.lamp_y_plugin.turn_off()

        await self.smart_home1.execute("turn_on", all=True)

        await asyncio.sleep(2)

        self.assertTrue(self.lamp_x_plugin.is_on, "'lamp_x_plugin' is off, but it should be turned on")
        self.assertTrue(self.lamp_y_plugin.is_off, "'lamp_y_plugin' is on, but message was incompatible")     # because no message was used during execute, which is compatible



