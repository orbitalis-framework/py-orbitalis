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
        name="lowercase",
        input=Input.from_message(StringMessage),
        output=Output.from_message(StringMessage)
    )
    async def lowercase_event_handler(self, topic: str, event: Event[StringMessage]):
        lowercase_text = event.payload.value.lower()

        connections = self._retrieve_connections(input_topic=topic, operation_name="lowercase")

        tasks = []
        for connection in connections:
            if connection.has_output:
                tasks.append(
                    asyncio.create_task(
                        self.eventbus_client.publish(
                            connection.output_topic,
                            lowercase_text
                        )
                    )
                )

        await asyncio.gather(*tasks)



