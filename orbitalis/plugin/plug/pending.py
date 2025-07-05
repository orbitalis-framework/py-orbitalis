from dataclasses import dataclass, field
import datetime
from typing import Set
from abc import ABC

from orbitalis.core.descriptor import CoreDescriptor
from orbitalis.orb.plug.pending import PendingRequest


@dataclass(frozen=True)
class CorePendingRequest(PendingRequest):
    core_descriptor: CoreDescriptor



