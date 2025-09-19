import asyncio

from busline.client.pubsub_client import PubSubClientBuilder
from busline.local.eventbus.local_eventbus import LocalEventBus
from busline.local.local_publisher import LocalPublisher
from busline.local.local_subscriber import LocalSubscriber

from examples.lamp.plugin.lamp_plugin import StatusMessage
from examples.lamp.plugin.lamp_x_plugin import LampXPlugin
from examples.lamp.core.smarthome_core import SmartHomeCore
from orbitalis.core.requirement import Constraint, OperationRequirement
from orbitalis.orbiter.schemaspec import Input, Output
from orbitalis.plugin.operation import Policy



async def main():
    """
    "smart_home1" can be compliance, "smart_home2" no due to "turn_on" operation of "lamp_x_plugin" which can serve only
    "smart_home1".
    """

    # Create a lamp X plugin (local message passing) with a custom policy which allows only one smart home core to be connected
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
        policy=Policy(allowlist=["smart_home1"])
    )

    lamp_x_plugin.eventbus_client.subscribers[0].identifier = "lamp_x_plugin_subscriber"

    assert "turn_on" in lamp_x_plugin.operations
    assert "turn_off" in lamp_x_plugin.operations
    assert "get_status" in lamp_x_plugin.operations

    # Create 2 smart home cores (local message passing)
    smart_home1 = SmartHomeCore(
        identifier="smart_home1",
        eventbus_client=PubSubClientBuilder()
            .with_subscriber(LocalSubscriber(eventbus=LocalEventBus()))
            .with_publisher(LocalPublisher(eventbus=LocalEventBus()))
            .build(),
        raise_exceptions=True,
        with_loop=False,
        operation_requirements={
            "turn_on": OperationRequirement(Constraint(
                minimum=1,
                mandatory=[lamp_x_plugin.identifier],
                inputs=[Input.empty()],
                outputs=[Output.no_output()]
            )),
            "turn_off": OperationRequirement(
                Constraint(
                    minimum=1,
                    mandatory=[lamp_x_plugin.identifier],
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

    smart_home1.eventbus_client.subscribers[0].identifier = "smart_home1_subscriber"

    smart_home2 = SmartHomeCore(
        identifier="smart_home2",
        eventbus_client=PubSubClientBuilder()
            .with_subscriber(LocalSubscriber(eventbus=LocalEventBus()))
            .with_publisher(LocalPublisher(eventbus=LocalEventBus()))
            .build(),
        raise_exceptions=True,
        with_loop=False,
        operation_requirements={
            "turn_on": OperationRequirement(Constraint(
                minimum=1,
                mandatory=[lamp_x_plugin.identifier],
                inputs=[Input.empty()],
                outputs=[Output.no_output()]
            )),
            "turn_off": OperationRequirement(Constraint(
                minimum=1,
                mandatory=[lamp_x_plugin.identifier],
                inputs=[Input.empty()],
                outputs=[Output.no_output()]
            )),
        }
    )

    smart_home2.eventbus_client.subscribers[0].identifier = "smart_home2_subscriber"

    # === HANDSHAKE ===
    assert not smart_home1.is_compliant()

    await lamp_x_plugin.start()
    await smart_home1.start()
    await smart_home2.start()

    await asyncio.sleep(1)  # await handshake process

    # Check handshake: `smart_home1` must have a connection with `smart_home1` for operations "turn_on", "turn_off" and "get_status"
    assert smart_home1.is_compliant()

    assert (len(smart_home1.retrieve_connections(
        remote_identifier=lamp_x_plugin.identifier,
        operation_name="turn_on"
    )) == 1)

    assert (len(smart_home1.retrieve_connections(
        remote_identifier=lamp_x_plugin.identifier,
        operation_name="turn_off"
    )) == 1)

    assert (len(smart_home1.retrieve_connections(
        remote_identifier=lamp_x_plugin.identifier,
        operation_name="get_status"
    )) == 1)

    assert (len(lamp_x_plugin.retrieve_connections(
        remote_identifier=smart_home1.identifier,
        operation_name="turn_on"
    )) == 1)

    assert (len(lamp_x_plugin.retrieve_connections(
        remote_identifier=smart_home1.identifier,
        operation_name="turn_off"
    )) == 1)

    # === Turn on lamp: execute "turn_on" ===

    lamp_x_plugin.turn_off()
    assert lamp_x_plugin.is_off

    await smart_home1.execute("turn_on", plugin_identifier=lamp_x_plugin.identifier)

    await asyncio.sleep(1)

    assert lamp_x_plugin.is_on


    # === CLOSE GRACELESS ===

    await smart_home1.send_graceless_close_connection(lamp_x_plugin.identifier, "turn_on")

    await asyncio.sleep(1)

    assert (len(smart_home1.retrieve_connections(
        remote_identifier=lamp_x_plugin.identifier,
        operation_name="turn_on"
    )) == 0)

    assert not smart_home1.is_compliant()

    await lamp_x_plugin.send_graceless_close_connection(smart_home1.identifier, "turn_off")

    await asyncio.sleep(1)

    assert (len(smart_home1.retrieve_connections(
        remote_identifier=lamp_x_plugin.identifier,
        operation_name="turn_off"
    )) == 0)

    assert not smart_home1.is_compliant()

    # === Turn on lamp, but should not work (due to close) ===

    lamp_x_plugin.turn_off()
    assert lamp_x_plugin.is_off

    await smart_home1.execute("turn_on", plugin_identifier=lamp_x_plugin.identifier)

    await asyncio.sleep(1)

    assert lamp_x_plugin.is_off

    # === RECONNECT ===

    await smart_home1.send_discover_based_on_requirements()

    await asyncio.sleep(1)

    assert (smart_home1.is_compliant())

    # === CLOSE GRACEFUL ===

    await lamp_x_plugin.send_graceful_close_connection(
        smart_home1.identifier,
        "turn_on"
    )

    await asyncio.sleep(1)

    assert not smart_home1.is_compliant()

    await asyncio.gather(
        lamp_x_plugin.stop(),
        smart_home1.stop(),
    )

    await asyncio.sleep(2)


if __name__ == '__main__':
    asyncio.run(main())