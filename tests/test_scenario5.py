import asyncio
import unittest

from busline.client.pubsub_client import PubSubClientBuilder
from busline.mqtt.mqtt_publisher import MqttPublisher
from busline.mqtt.mqtt_subscriber import MqttSubscriber
from orbitalis.core.need import Constraint, Need
from orbitalis.core.state import CoreState
from orbitalis.orbiter.schemaspec import Input, Output
from orbitalis.plugin.operation import Policy
from tests.core.smarthome_core import SmartHomeCore
from tests.plugin.lamp.lamp_x_plugin import LampXPlugin


class TestPlugin(unittest.IsolatedAsyncioTestCase):
    """
    Both "smart_home1" "smart_home2" can be compliance.
    """


    async def test_handshake_and_execution(self):
        
        lamp_x_plugin = LampXPlugin(
            identifier="lamp_x_plugin",
            eventbus_client=PubSubClientBuilder().with_subscriber(MqttSubscriber(hostname="127.0.0.1")).with_publisher(MqttPublisher(hostname="127.0.0.1")).build(),
            raise_exceptions=True,
            with_loop=False,

            kwh=24      # LampPlugin-specific attribute
        ).with_custom_policy(
            operation_name="turn_on",
            policy=Policy(maximum=2)
        )

        self.assertTrue("turn_on" in lamp_x_plugin.operations)
        self.assertTrue("turn_off" in lamp_x_plugin.operations)
        self.assertTrue("get_status" in lamp_x_plugin.operations)

        smart_home = SmartHomeCore(
            identifier="smart_home",
            eventbus_client=PubSubClientBuilder().with_subscriber(MqttSubscriber(hostname="127.0.0.1")).with_publisher(
                MqttPublisher(hostname="127.0.0.1")).build(),
            raise_exceptions=True,
            with_loop=False,
            needed_operations={
                "turn_on": Need(Constraint(
                    minimum=1,
                    mandatory=[lamp_x_plugin.identifier],
                    inputs=[Input.empty()],
                    outputs=[Output.no_output()]
                )),
                "turn_off": Need(Constraint(
                    minimum=1,
                    mandatory=[lamp_x_plugin.identifier],
                    inputs=[Input.empty()],
                    outputs=[Output.no_output()]
                )),
            }
        )
        
        self.assertFalse(smart_home.is_compliance())

        await lamp_x_plugin.start()
        await smart_home.start()

        await asyncio.sleep(2)

        self.assertTrue(smart_home.is_compliance())
        self.assertEqual(smart_home.state, CoreState.COMPLIANT)

        lamp_x_plugin.turn_off()
        self.assertTrue(lamp_x_plugin.is_off)

        await smart_home.execute(
            "turn_on",
            all=True
        )

        await asyncio.sleep(2)

        self.assertTrue(lamp_x_plugin.is_on)

        lamp_x_plugin.turn_off()
        self.assertTrue(lamp_x_plugin.is_off)

        await lamp_x_plugin.stop()

        await smart_home.execute(
            "turn_on",
            plugin_identifier=lamp_x_plugin.identifier
        )

        await asyncio.sleep(2)

        self.assertTrue(lamp_x_plugin.is_off)

        await smart_home.stop()

        await asyncio.sleep(2)



