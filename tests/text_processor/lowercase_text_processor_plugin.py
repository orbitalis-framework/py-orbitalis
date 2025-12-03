import asyncio
from dataclasses import dataclass
from busline.event.event import Event
from busline.event.message.string_message import StringMessage

from orbitalis.orbiter.schemaspec import Input, Output
from orbitalis.plugin.operation import operation
from orbitalis.plugin.plugin import Plugin


@dataclass
class LowercaseTextProcessorPlugin(Plugin):

    @operation(
        name="lowercase",       # operation's name
        input=Input.from_message(StringMessage),    # operation's input
        output=Output.from_message(StringMessage)   # operation's output
        # no policy is specified => Policy.no_constraints()
    )
    async def lowercase_event_handler(self, topic: str, event: Event[StringMessage]):
        # NOTE: input message specified in @operation should be the same of
        # what is specified as type hint of event parameter

        # Retrieve input string value, remember that it is wrapped into StringMessage
        input_str = event.payload.value

        lowercase_text = input_str.lower()  # process the string

        # Retrieve and touch related connections
        connections = await self.retrieve_and_touch_connections(
            input_topic=topic,
            operation_name="lowercase"
        )

        tasks = []
        for connection in connections:

            # Only if the connection expects an output
            # it is published in the related topic
            # specified by `connection.output_topic`
            if connection.has_output:
                tasks.append(
                    asyncio.create_task(
                        self.eventbus_client.publish(
                            connection.output_topic,
                            lowercase_text  # will be wrapped into StringMessage
                        )
                    )
                )

        await asyncio.gather(*tasks)    # wait publishes



