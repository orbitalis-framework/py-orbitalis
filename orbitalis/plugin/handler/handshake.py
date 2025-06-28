from dataclasses import dataclass
from typing import override, TYPE_CHECKING
from busline.client.subscriber.event_handler.event_handler import EventHandler
from orbitalis.events.handshake.discover import DiscoverEvent
from orbitalis.events.handshake.offer import OfferEvent, OfferEventContent
from orbitalis.events.handshake.reply import ReplyEvent
from orbitalis.events.wellknown_topic import WellKnownHandShakeTopic
from orbitalis.plugin.plug.pending import CorePendingRequest

if TYPE_CHECKING:
    from orbitalis.plugin.plugin import Plugin


@dataclass
class DiscoverHandler(EventHandler):

    context: 'Plugin'


    @override
    async def handle(self, topic: str, event: DiscoverEvent):

        # TODO: use priority to disconnect from a low priority core, if high priority core incomes

        if self.context.is_core_compatible(event.content.core_descriptor):

            await self.context.send_offer(
                offer_topic=event.content.offer_topic,
                core_descriptor=event.content.core_descriptor
            )


@dataclass
class ReplyHandler(EventHandler):

    context: 'Plugin'

    @override
    async def handle(self, topic: str, event: ReplyEvent):
        # TODO
        pass
