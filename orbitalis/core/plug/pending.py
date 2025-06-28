from dataclasses import dataclass, field
import datetime
from typing import Set
from abc import ABC

from orbitalis.orb.plug.pending import PendingRequest
from orbitalis.plugin.descriptor import PluginDescriptor


@dataclass(frozen=True)
class PluginPendingRequest(PendingRequest):
    plugin_descriptor: PluginDescriptor



