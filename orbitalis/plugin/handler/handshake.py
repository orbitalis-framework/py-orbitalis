from dataclasses import dataclass
from typing import override, TYPE_CHECKING
from busline.client.subscriber.event_handler.event_handler import EventHandler
from orbitalis.events.handshake.discover import DiscoverEvent
from orbitalis.events.wellknown_topic import WellKnownHandShakeTopic

if TYPE_CHECKING:
    from orbitalis.plugin.plugin import Plugin


@dataclass
class DiscoverHandler(EventHandler):

    context: 'Plugin'


    @override
    async def handle(self, topic: str, event: DiscoverEvent):

        # TODO: use priority to disconnect from a low priority core, if high priority core incomes

        if self.context.is_core_compatible(event.content.core_descriptor):
            self.context.eventbus_client.publish(WellKnownHandShakeTopic.build_offer_topic(event.content.core_descriptor.identifier))
