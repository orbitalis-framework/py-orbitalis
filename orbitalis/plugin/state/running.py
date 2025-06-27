from dataclasses import dataclass, field
from typing import override, Self, TYPE_CHECKING
from orbitalis.plugin.state.base import PluginState
from orbitalis.state_machine.state_machine import PreventInitStateMixin

if TYPE_CHECKING:
    from orbitalis.plugin.plugin import Plugin


@dataclass
class Running(PluginState, PreventInitStateMixin):
    name: str = field(default="RUNNING", init=False)

    @override
    async def handle(self, *args, **kwargs):
        return self

    @classmethod
    async def _internal_create(cls, context: 'Plugin', *args, **kwargs) -> Self:
        state = Running(context)
        await context.subscribe_on_discover()
        return state
