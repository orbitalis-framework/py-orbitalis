import asyncio
from typing import FrozenSet, override
from abc import ABC
from dataclasses import dataclass, field

from orbitalis.core.descriptor import CoreDescriptor
from orbitalis.descriptor.descriptors_manager import DescriptorsManager
from orbitalis.events.wellknown_topic import WellKnownHandShakeTopic
from orbitalis.orb.orb import Orb
from orbitalis.plugin.configuration import PluginConfiguration
from orbitalis.plugin.descriptor import PluginDescriptor
from orbitalis.plugin.state.running import Running
from orbitalis.state_machine.state_machine import StateMachine, State


@dataclass(kw_only=True, frozen=True)
class Plugin(Orb, StateMachine, ABC):
    configuration: PluginConfiguration = field(default_factory=PluginConfiguration)
    categories: FrozenSet[str] = field(default_factory=frozenset)
    core_descriptors: DescriptorsManager = field(default_factory=DescriptorsManager)

    @override
    async def _internal_start(self, *args, **kwargs):
        await super()._internal_start(*args, **kwargs)
        self.set_state(await Running.create(self))

    async def subscribe_on_discover(self):
        # TODO: gestione eccezioni
        # TODO: gestione on_event

        # unsubscribe from previous
        await self.eventbus_client.unsubscribe(WellKnownHandShakeTopic.build_discover_topic())

        if self.configuration.allowlist is not None:
            tasks = [self.eventbus_client.subscribe(WellKnownHandShakeTopic.build_discover_topic(core_id)) for core_id in self.configuration.allowlist]
            await asyncio.gather(*tasks)

        else:
            await self.eventbus_client.subscribe(WellKnownHandShakeTopic.build_discover_topic())

    def is_core_compatible(self, core_descriptor: CoreDescriptor) -> bool:

        if self.configuration.maximum - len(self.core_descriptors) <= 0:
            return False

        if self.configuration.blocklist is not None and core_descriptor.identifier in self.configuration.blocklist:
            return False

        if self.configuration.allowlist is not None and core_descriptor.identifier not in self.configuration.allowlist:
            return False

        return True

    def generate_descriptor(self) -> PluginDescriptor:
        return PluginDescriptor(
            identifier=self.identifier,
            categories=self.categories
        )


