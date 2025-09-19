import asyncio

from busline.client.pubsub_client import PubSubClientBuilder
from busline.local.eventbus.local_eventbus import LocalEventBus
from busline.local.local_publisher import LocalPublisher
from busline.local.local_subscriber import LocalSubscriber

from examples.lamp.core.smarthome_core import SmartHomeCore
from examples.lamp.plugin.lamp_x_plugin import LampXPlugin
from examples.lamp.plugin.lamp_y_plugin import LampYPlugin, TurnOnLampYMessage, TurnOffLampYMessage
from orbitalis.core.requirement import Constraint, OperationRequirement
from orbitalis.orbiter.schemaspec import Input, Output
from orbitalis.plugin.operation import Policy


async def main():
    """
    Use multiple plugins with the same core
    """

    lamp_x_plugin = LampXPlugin(
        identifier="lamp_x_plugin",
        eventbus_client=PubSubClientBuilder()
            .with_subscriber(LocalSubscriber(eventbus=LocalEventBus()))
            .with_publisher(LocalPublisher(eventbus=LocalEventBus()))
            .build(),
        raise_exceptions=True,
        with_loop=False,

        kw=24  # LampPlugin-specific attribute
    ).with_custom_policy(
        operation_name="turn_on",
        policy=Policy(maximum=2)
    )

    assert "turn_on" in lamp_x_plugin.operations
    assert "turn_off" in lamp_x_plugin.operations
    assert "get_status" in lamp_x_plugin.operations

    lamp_y_plugin = LampYPlugin(
        identifier="lamp_y_plugin",
        eventbus_client=PubSubClientBuilder()
            .with_subscriber(LocalSubscriber(eventbus=LocalEventBus()))
            .with_publisher(LocalPublisher(eventbus=LocalEventBus()))
            .build(),
        raise_exceptions=True,
        with_loop=False,

        # LampPlugin-specific attributes
        kw=24,
        power=0.1
    ).with_custom_policy(
        operation_name="turn_on",
        policy=Policy(maximum=2)
    )

    assert "turn_on" in lamp_y_plugin.operations
    assert "turn_off" in lamp_y_plugin.operations
    assert "get_status" in lamp_y_plugin.operations

    smart_home = SmartHomeCore(
        identifier="smart_home",
        eventbus_client=PubSubClientBuilder()
            .with_subscriber(LocalSubscriber(eventbus=LocalEventBus()))
            .with_publisher(LocalPublisher(eventbus=LocalEventBus()))
            .build(),
        raise_exceptions=True,
        with_loop=False,
        operation_requirements={
            "turn_on": OperationRequirement(Constraint(
                minimum=1,
                mandatory=[lamp_x_plugin.identifier, lamp_y_plugin.identifier],
                inputs=[Input.empty(), Input.from_message(TurnOnLampYMessage)],     # both lamp X and lamp Y inputs
                outputs=[Output.no_output()]
            )),
            "turn_off": OperationRequirement(Constraint(
                minimum=1,
                mandatory=[lamp_x_plugin.identifier, lamp_y_plugin.identifier],
                inputs=[Input.empty(), Input.from_schema(TurnOffLampYMessage.avro_schema())],     # both lamp X and lamp Y inputs
                outputs=[Output.no_output()]
            )),
        }
    )
    
    assert not smart_home.is_compliant()    # obviously, no headshake performed

    # Start orbiters to trigger handshake
    await lamp_x_plugin.start()
    await lamp_y_plugin.start()
    await smart_home.start()

    await asyncio.sleep(2)  # await handshake process

    assert smart_home.is_compliant()    # `smart_home` core should be connected to both lamp plugins

    # Check lamps statuses
    assert lamp_x_plugin.is_off
    assert lamp_y_plugin.is_off

    # === GOAL: Turn on *both* lamps ===

    # Call "turn_on" on *all* plugins which are compatible with `data` argument type, i.e. `None`
    # ...only `lamp_x_plugin`
    await smart_home.execute(
        operation_name="turn_on",
        all=True
    )

    await asyncio.sleep(2)      # await communication

    # Check lamps statuses
    assert lamp_x_plugin.is_on
    assert lamp_y_plugin.is_off

    # Turn on `lamp_y_plugin` explicitly (NOTE: now `data` is required)
    await smart_home.execute(
        operation_name="turn_on",
        plugin_identifier=lamp_y_plugin.identifier,
        data=TurnOnLampYMessage(power=0.5)  # remember `power` between 0 and 1
    )

    # Check lamps statuses
    assert lamp_x_plugin.is_on
    assert lamp_y_plugin.is_on

    # Stop orbiters after use
    await lamp_x_plugin.stop()
    await smart_home.stop()

    await asyncio.sleep(2)


if __name__ == '__main__':
    asyncio.run(main())
