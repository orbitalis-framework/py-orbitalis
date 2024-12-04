from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List


@dataclass
class PlugInformation:
    """

    Author: Nicola Ricciardi
    """

    plugin_identifier: str
    plugin_type: str
    plugged_at: datetime = field(default=datetime.now())



class PlugsInformation:

    def __init__(self):
        self.__plugs_information: List[PlugInformation] = []

    def remove(self, plug_identifier: str | None = None, plug_type: str | None = None):

        new_plugs_information: List[PlugInformation] = []

        for plug_info in self.__plugs_information:
            if plug_identifier is None or plug_info.plugin_identifier != plug_identifier:
                continue

            if plug_type is None or plug_info.plugin_type != plug_type:
                continue

            new_plugs_information.append(plug_info)


        self.__plugs_information = new_plugs_information

    def add(self, plug_information: PlugInformation):
        self.__plugs_information.append(plug_information)

    @property
    def plugged_plugins_by_types(self) -> Dict[str, List[PlugInformation]]:
        plugs_info_by_types = dict()

        for plug_info in self.__plugs_information:
            plugs_info_by_types.setdefault(plug_info.plugin_type, [])
            plugs_info_by_types[plug_info.plugin_type].append(plug_info)

        return plugs_info_by_types

    @property
    def plugged_plugins_by_identifiers(self) -> Dict[str, PlugInformation]:
        plugs_info_by_identifiers = dict()

        for plug_info in self.__plugs_information:
            plugs_info_by_identifiers[plug_info.plugin_identifier] = plug_info

        return plugs_info_by_identifiers