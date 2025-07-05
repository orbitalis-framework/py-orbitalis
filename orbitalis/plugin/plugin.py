from typing import FrozenSet, override, Set, Dict, Self
from abc import ABC
from dataclasses import dataclass, field

from busline.client.subscriber.event_handler.schemafull_handler import SchemafullEventHandler
from orbitalis.core.descriptor import CoreDescriptor
from orbitalis.events.handshake.offer import OfferMessage
from orbitalis.orb.descriptor.descriptors_manager import DescriptorsManager
from orbitalis.events.wellknown_topic import WellKnownHandShakeTopic
from orbitalis.orb.orb import Orb
from orbitalis.plugin.configuration import PluginConfiguration
from orbitalis.plugin.descriptor import PluginDescriptor
from orbitalis.plugin.handler.handshake import DiscoverHandler, ReplyHandler
from orbitalis.plugin.plug.pending import CorePendingRequest
from orbitalis.plugin.state.running import Running
from orbitalis.state_machine.state_machine import StateMachine
from orbitalis.utils.service import Feature, Service


@dataclass(kw_only=True)
class Plugin(Orb, StateMachine, Service, ABC):
    """

    Author: Nicola Ricciardi
    """

    configuration: PluginConfiguration = field(default_factory=PluginConfiguration)
    core_descriptors: DescriptorsManager = field(default_factory=DescriptorsManager, init=False)
    pending_requests: Dict[str, CorePendingRequest] = field(default_factory=dict, init=False)   # core_identifier => CorePendingRequest

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
        self.state = await Running.create(self)

    async def subscribe_on_discover(self):
        # TODO: gestione eccezioni
        # TODO: gestione on_event

        await self.eventbus_client.subscribe(WellKnownHandShakeTopic.DISCOVER_TOPIC)

    def can_plug_into_core(self, core_descriptor: CoreDescriptor) -> bool:

        available_slots: int = self.configuration.acceptance_policy.maximum

        available_slots -= len(self.core_descriptors)       # already plugged

        available_slots -= len(self.pending_requests)       # pending requests

        if available_slots <= 0:
            return False

        if (self.configuration.acceptance_policy.blocklist is not None
                and core_descriptor.identifier in self.configuration.acceptance_policy.blocklist):
            return False

        if (self.configuration.acceptance_policy.allowlist is not None
                and core_descriptor.identifier not in self.configuration.acceptance_policy.allowlist):
            return False

        return True

    async def plug_into_core(self, core_descriptor: CoreDescriptor):
        pass    # TODO

    async def send_offer(self, offer_topic: str, core_descriptor: CoreDescriptor):

        reply_topic: str = WellKnownHandShakeTopic.build_reply_topic(
            core_identifier=core_descriptor.identifier,
            plugin_identifier=self.identifier
        )

        await self.eventbus_client.subscribe(
            topic=reply_topic,
            handler=self.reply_handler
        )

        await self.eventbus_client.publish(
            topic=offer_topic,
            event=OfferMessage(
                plugin_descriptor=self.generate_descriptor(),
                allowlist=self.configuration.acceptance_policy.allowlist,
                blocklist=self.configuration.acceptance_policy.blocklist,
                reply_topic=reply_topic
            ).into_event()
        )

        self.pending_requests[core_descriptor.identifier] = CorePendingRequest(
            core_descriptor=core_descriptor,
            related_topics=set(reply_topic)
        )

    @override
    def add_context_to_feature(self, feature: Feature[Self]):
        feature.context = self

    @override
    def generate_descriptor(self) -> PluginDescriptor:
        return PluginDescriptor(
            identifier=self.identifier,
            categories=self.configuration.categories,
            features=self.generate_features_description()
        )


