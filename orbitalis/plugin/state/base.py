from abc import ABC
from dataclasses import dataclass, field
from typing import override, Self, TYPE_CHECKING
from orbitalis.state_machine.state_machine import StateMachine, State, PreventInitStateMixin

if TYPE_CHECKING:
    from orbitalis.plugin.plugin import Plugin


@dataclass
class PluginState(State, ABC):
    context: 'Plugin'

