from dataclasses import dataclass
from typing import override, TYPE_CHECKING
from busline.client.subscriber.event_handler.event_handler import EventHandler
from orbitalis.events.handshake.discover import DiscoverMessage
from orbitalis.events.handshake.offer import OfferMessage
from orbitalis.events.handshake.reply import ReplyMessage
from orbitalis.events.wellknown_topic import WellKnownHandShakeTopic
from orbitalis.plugin.handler.plugin_handler import PluginHandler
from orbitalis.plugin.plug.pending import CorePendingRequest


@dataclass
class DiscoverHandler(PluginHandler):

    @override
    async def handle(self, topic: str, event: DiscoverMessage):

        # TODO: use priority to disconnect from a low priority core, if high priority core incomes

        if self.context.is_core_compatible(event.content.core_descriptor):

            await self.context.send_offer(
                offer_topic=event.content.offer_topic,
                core_descriptor=event.content.core_descriptor
            )


@dataclass
class ReplyHandler(PluginHandler):

    @override
    async def handle(self, topic: str, event: ReplyMessage):
        # TODO
        pass
