from dataclasses import dataclass
from typing import override, TYPE_CHECKING
from busline.client.subscriber.event_handler.event_handler import EventHandler

from orbitalis.core.plug.pending import PluginPendingRequest
from orbitalis.events.handshake.offer import OfferEvent, OfferMessage
from orbitalis.events.handshake.reply import ReplyEvent, ReplyMessage
from orbitalis.events.wellknown_topic import WellKnownHandShakeTopic

if TYPE_CHECKING:
    from orbitalis.core.core import Core


@dataclass
class OfferHandler(EventHandler):

    context: 'Core'


    @override
    async def handle(self, topic: str, event: OfferEvent):

        if self.context.is_plugin_needed_and_pluggable(event.content.plugin_descriptor):

            response_topic: str = WellKnownHandShakeTopic.build_response_topic(
                self.context.identifier,
                event.content.plugin_descriptor.identifier
            )

            await self.context.eventbus_client.subscribe(
                topic=response_topic,
                handler=self.context.response_handler
            )

            await self.context.eventbus_client.publish(
                topic=event.content.reply_topic,
                event=ReplyEvent(
                    content=ReplyMessage(
                        plug_request=True,
                        description="I utils you",
                        response_topic=response_topic
                    )
                )
            )

            self.context.pending_requests.add(
                PluginPendingRequest(
                    plugin_descriptor=event.content.plugin_descriptor,
                    related_topics=set(response_topic)
                )
            )

        else:
            await self.context.eventbus_client.publish(
                topic=event.content.reply_topic,
                event=ReplyEvent(
                    content=ReplyMessage(
                        plug_request=False,
                        description="Not needed anymore, sorry",
                    )
                )
            )

@dataclass
class ResponseHandler(EventHandler):

    context: 'Core'

    @override
    async def handle(self, topic: str, event: OfferEvent):
        # TODO
        pass