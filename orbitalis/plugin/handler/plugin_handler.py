from dataclasses import dataclass
from typing import override, TYPE_CHECKING, Self
from busline.client.subscriber.event_handler.event_handler import EventHandler
from abc import ABC

if TYPE_CHECKING:
    from orbitalis.plugin.plugin import Plugin


@dataclass
class PluginHandler(EventHandler, ABC):

    context: 'Plugin'

    @classmethod
    def from_plugin(cls, plugin: 'Plugin') -> Self:
        return cls(plugin)