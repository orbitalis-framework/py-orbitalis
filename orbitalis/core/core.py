import asyncio
from abc import ABC

from typing import Dict, List

from busline.eventbus_client.eventbus_client import EventBusClient
from orbitalis.core.configuration import CoreConfiguration
from orbitalis.core.plug_info import PlugInformation, PlugsInformation
from orbitalis.core.state import CoreState
from orbitalis.events.discover import DiscoverEvent, DiscoverEventContent
from orbitalis.wellknown_topic import CorePluginCommunicationTopic


class Core(ABC):

    def __init__(self, identifier: str, eventbus_client: EventBusClient, configuration: CoreConfiguration):

        self._identifier = identifier
        self._eventbus_client = eventbus_client
        self._configuration = configuration
        self._state = CoreState.CREATED
        self._plugs_information: PlugsInformation = PlugsInformation()

        self.__state_management_loop()


    async def __state_management_loop(self):

        while self._state != CoreState.STOPPED:

            # TODO:
            # for needed_type, needed_conf in self._configuration.needed_plugins_by_type:
            #     n_plugs = len(self._plugs_information.plugged_plugins_by_types[needed_type])
            #
            #     if needed_conf.min > n_plugs:
            #         pass
            #         # TODO: send discover
            #
            #     if needed_conf.max < n_plugs:
            #         pass
            #         # TODO: send unplug

            for plug_id in self._configuration.needed_plugins_by_identifier:
                if plug_id not in self._plugs_information.plugged_plugins_by_identifiers:
                    await self._eventbus_client.publish(
                        CorePluginCommunicationTopic.PLUGIN_DISCOVER.name,
                        DiscoverEvent(DiscoverEventContent(
                            core_identifier=self._identifier,
                            identifiers=[plug_id]
                        ))
                    )


            await asyncio.sleep(self._configuration.discovering_interval)


