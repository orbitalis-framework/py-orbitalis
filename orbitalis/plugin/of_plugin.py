from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Self, Optional
from abc import ABC

if TYPE_CHECKING:
    from orbitalis.plugin.plugin import Plugin


@dataclass
class OfPluginMixin(ABC):

    context: Optional['Plugin'] = field(default=None, repr=False)

    @classmethod
    def from_plugin(cls, plugin: 'Plugin') -> Self:
        return cls(plugin)