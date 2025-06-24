import asyncio
from typing import FrozenSet, override
from abc import ABC
from dataclasses import dataclass, field
from orbitalis.descriptor.descriptor import Descriptor
from orbitalis.events.wellknown_topic import WellKnownHandShakeTopic
from orbitalis.orb.orb import Orb
from orbitalis.plugin.configuration import PluginConfiguration
from orbitalis.plugin.descriptor import PluginDescriptor
from orbitalis.state_machine.state_machine import StateMachine, State


@dataclass
class Plugin(Orb, StateMachine, ABC):
    configuration: PluginConfiguration = field(default_factory=PluginConfiguration)
    tags: FrozenSet[str] = field(default_factory=frozenset)

    @override
    async def _internal_start(self, *args, **kwargs):
        await Running.mount(self)

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


@dataclass
class PluginState(State, ABC):
    context: Plugin


@dataclass
class Running(PluginState):
    name: str = field(default="RUNNING", init=False)

    _allow_init: bool = False

    def __post_init__(self):
        if self._allow_init:
            raise NotImplemented("Canonical constructor is not available for this class, use mount instead")

    @override
    async def handle(self, *args, **kwargs):
        return self

    @classmethod
    async def mount(cls, context: Plugin, *args, **kwargs):
        cls._allow_init = True

        context.state = Running(context)
        await context.subscribe_on_discover()

        cls._allow_init = False
