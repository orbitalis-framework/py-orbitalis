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

    def setUp(self):

        self.smart_home = SmartHomeCore(
            identifier="smart_home",
            eventbus_client=build_new_local_client(),
            raise_exceptions=True,
            with_loop=False,
            operation_requirements={
                "turn_on": OperationRequirement(Constraint(
                    minimum=1,
                    inputs=[Input.empty()],
                    outputs=[Output.no_output()]
                )),
                "turn_off": OperationRequirement(
                    Constraint(
                        minimum=1,
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

    async def test_execute_modes(self):

        plugin1 = LampXPlugin(
            identifier="plugin1",
            eventbus_client=build_new_local_client(),
            raise_exceptions=True,
            with_loop=False,

            kw=24      # LampPlugin-specific attribute
        )

        plugin2 = LampXPlugin(
            identifier="plugin2",
            eventbus_client=build_new_local_client(),
            raise_exceptions=True,
            with_loop=False,

            kw=42      # LampPlugin-specific attribute
        )

        await plugin1.start()
        await plugin2.start()


        self.assertFalse(self.smart_home.is_compliant())   # already compliance, but can plug plugins

        await self.smart_home.start()

        await self.smart_home.new_connection_added_event.wait()

        await self.smart_home.new_connection_added_event.wait()

        self.assertTrue(self.smart_home.is_compliant())

        self.assertEqual(len(self.smart_home.retrieve_connections(operation_name="turn_on")), 2)
        self.assertEqual(len(self.smart_home.retrieve_connections(operation_name="turn_off")), 2)
        self.assertEqual(len(self.smart_home.retrieve_connections(operation_name="get_status")), 2)

        # === USING PLUGIN ===
        plugin1.turn_off()
        self.assertTrue(plugin1.is_off)

        plugin2.turn_off()
        self.assertTrue(plugin2.is_off)

        self.assertTrue(
            await self.smart_home.execute_using_plugin(
                operation_name="turn_on",
                plugin_identifier="plugin1"
            )
        )

        self.assertTrue(plugin1.is_on)
        self.assertTrue(plugin2.is_off)

        # === ANY ===
        plugin1.turn_off()
        self.assertTrue(plugin1.is_off)

        plugin2.turn_off()
        self.assertTrue(plugin2.is_off)

        plugin_identifier = await self.smart_home.execute_sending_any(operation_name="turn_on")

        self.assertTrue(
            plugin_identifier == plugin1.identifier or plugin_identifier == plugin2.identifier
        )

        self.assertTrue(
            plugin1.is_on or plugin2.is_on
        )

        self.assertFalse(
            plugin1.is_on and plugin2.is_on
        )

        # === ALL ===
        plugin1.turn_off()
        self.assertTrue(plugin1.is_off)

        plugin2.turn_off()
        self.assertTrue(plugin2.is_off)

        plugin_identifiers = await self.smart_home.execute_sending_all(operation_name="turn_on")

        self.assertTrue(
            plugin1.is_on and plugin2.is_on
        )

        self.assertIn(plugin_identifier, plugin_identifiers)

        self.assertIn(plugin2.identifier, plugin_identifiers)

        # === DISTRIBUTED ===
        plugin1.turn_off()
        self.assertTrue(plugin1.is_off)

        plugin2.turn_off()
        self.assertTrue(plugin2.is_off)

        plugin_identifiers = await self.smart_home.execute_distributed(
            operation_name="turn_on",
            data=[None, None, None, None]
        )

        self.assertTrue(
            plugin1.is_on and plugin2.is_on
        )

        self.assertIn(plugin_identifier, plugin_identifiers)

        self.assertIn(plugin2.identifier, plugin_identifiers)

        await asyncio.sleep(1)



if __name__ == "__main__":
    unittest.main()