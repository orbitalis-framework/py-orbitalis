from typing import FrozenSet, override, Set
from abc import ABC
from dataclasses import dataclass, field

from orbitalis.core.descriptor import CoreDescriptor
from orbitalis.events.handshake.offer import OfferEvent, OfferEventContent
from orbitalis.orb.descriptor.descriptors_manager import DescriptorsManager
from orbitalis.events.wellknown_topic import WellKnownHandShakeTopic
from orbitalis.orb.orb import Orb
from orbitalis.plugin.configuration import PluginConfiguration
from orbitalis.plugin.descriptor import PluginDescriptor
from orbitalis.plugin.handler.handshake import DiscoverHandler, ReplyHandler
from orbitalis.plugin.plug.pending import CorePendingRequest
from orbitalis.plugin.state.running import Running
from orbitalis.state_machine.state_machine import StateMachine


@dataclass(kw_only=True)
class Plugin(Orb, StateMachine, ABC):
    """

    Author: Nicola Ricciardi
    """

    configuration: PluginConfiguration = field(default_factory=PluginConfiguration)
    categories: FrozenSet[str] = field(default_factory=frozenset)
    core_descriptors: DescriptorsManager = field(default_factory=DescriptorsManager)
    pending_requests: Set[CorePendingRequest] = field(default_factory=set)

    def __post_init__(self):
        self._discover_handler = DiscoverHandler(self)
        self._reply_handler = ReplyHandler(self)

    @property
    def discover_handler(self) -> DiscoverHandler:
        return self._discover_handler

    @property
    def reply_handler(self) -> ReplyHandler:
        return self._reply_handler

    @override
    async def _internal_start(self, *args, **kwargs):
        await super()._internal_start(*args, **kwargs)
        self.set_state(await Running.create(self))

    async def subscribe_on_discover(self):
        # TODO: gestione eccezioni
        # TODO: gestione on_event

        await self.eventbus_client.subscribe(WellKnownHandShakeTopic.DISCOVER_TOPIC)

    def is_core_compatible(self, core_descriptor: CoreDescriptor) -> bool:

        if self.configuration.acceptance_policy.maximum - len(self.core_descriptors) <= 0:
            return False

        if (self.configuration.acceptance_policy.blocklist is not None
                and core_descriptor.identifier in self.configuration.acceptance_policy.blocklist):
            return False

        if (self.configuration.acceptance_policy.allowlist is not None
                and core_descriptor.identifier not in self.configuration.acceptance_policy.allowlist):
            return False

        return True


    async def send_offer(self, offer_topic: str, core_descriptor: CoreDescriptor):

        reply_topic: str = WellKnownHandShakeTopic.build_reply_topic(
            core_identifier=core_descriptor.identifier,
            plugin_identifier=self.context.identifier
        )

        await self.context.eventbus_client.subscribe(
            topic=reply_topic,
            handler=self.context.reply_handler
        )

        await self.context.eventbus_client.publish(
            topic=offer_topic,
            event=OfferEvent(content=OfferEventContent(
                plugin_descriptor=self.context.generate_descriptor(),
                allowlist=self.configuration.acceptance_policy.allowlist,
                blocklist=self.configuration.acceptance_policy.blocklist,
                reply_topic=reply_topic
            ))
        )

        self.context.pending_requests.add(CorePendingRequest(
            core_descriptor=core_descriptor,
            related_topics=set(reply_topic)
        ))

    def generate_descriptor(self) -> PluginDescriptor:
        return PluginDescriptor(
            identifier=self.identifier,
            categories=self.categories
        )


