import asyncio
from typing import FrozenSet, override
from abc import ABC
from dataclasses import dataclass

from orbitalis.descriptor.descriptor import Descriptor
from orbitalis.events.wellknown_topic import WellKnownHandShakeTopic
from orbitalis.orb.orb import Orb
from orbitalis.plugin.state import Running
from orbitalis.plugin.configuration import PluginConfiguration
from orbitalis.plugin.descriptor import PluginDescriptor


@dataclass
class Plugin(Orb, ABC):
    tags: FrozenSet[str]
    configuration: PluginConfiguration

    @override
    def _internal_start(self, *args, **kwargs):
        self.state = Running(self)

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


    def generate_descriptor(self) -> PluginDescriptor:
        return PluginDescriptor(
            identifier=self.identifier,
            tags=self.tags
        )