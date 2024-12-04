from abc import ABC
from typing import List

from busline.eventbus_client.eventbus_client import EventBusClient

from orbitalis.plugin.configuration import PluginConfiguration
from orbitalis.plugin.state import PluginState


class Plugin(ABC):

    def __init__(self, identifier: str, eventbus_client: EventBusClient, configuration: PluginConfiguration, types: List[str] | None = None):

        self._identifier = identifier
        self._eventbus_client = eventbus_client
        self._configuration = configuration
        self._types = types
        self._state = PluginState.CREATED




