import asyncio
from dataclasses import dataclass
from typing import Optional

from busline.client.pubsub_client import PubSubClientBuilder
from busline.client.subscriber.event_handler import CallbackEventHandler
from busline.event.event import Event
from busline.event.message.string_message import StringMessage
from busline.local.eventbus.local_eventbus import LocalEventBus
from busline.local.local_publisher import LocalPublisher
from busline.local.local_subscriber import LocalSubscriber

from examples.text_processor.lowercase_text_processor_plugin import LowercaseTextProcessorPlugin
from orbitalis.core.core import Core
from orbitalis.core.requirement import Constraint, OperationRequirement
from orbitalis.core.sink import sink
from orbitalis.orbiter.schemaspec import Input, Output


@dataclass
class MyCore(Core):
    last_result: Optional[str] = None

    @sink("lowercase")
    async def lowercase_sink(self, topic: str, event: Event[StringMessage]):
        """
        Collect lower case plugin's results
        """

        self.last_result = event.payload.value


async def main():

    # Create new plugin (local message passing)
    plugin = LowercaseTextProcessorPlugin(
        eventbus_client=PubSubClientBuilder()
            .with_subscriber(LocalSubscriber(eventbus=LocalEventBus()))
            .with_publisher(LocalPublisher(eventbus=LocalEventBus()))
            .build(),
        with_loop=False,
        raise_exceptions=True
    )

    # Create new core (local message passing)
    core = MyCore(
        eventbus_client=PubSubClientBuilder()
            .with_subscriber(LocalSubscriber(eventbus=LocalEventBus()))
            .with_publisher(LocalPublisher(eventbus=LocalEventBus()))
            .build(),
        with_loop=False,
        raise_exceptions=True,
        operation_requirements={
            "lowercase": OperationRequirement(Constraint(
                inputs=[Input.from_message(StringMessage)],
                outputs=[Output.from_message(StringMessage)],
            ))
        }
    )

    # Start orbiters and await handshake
    await plugin.start()
    await core.start()

    await asyncio.sleep(2)

    # Request to elaborate string message "HELLO", in order to transform it into "hello"
    await core.execute(
        operation_name="lowercase",
        data=StringMessage("HELLO"),
        any=True
    )

    await asyncio.sleep(1)  # await elaboration and response

    # Check result
    assert core.last_result, "hello"

    # Stop orbiters and await closing process
    await plugin.stop()
    await core.stop()

    await asyncio.sleep(1)


if __name__ == '__main__':
    asyncio.run(main())