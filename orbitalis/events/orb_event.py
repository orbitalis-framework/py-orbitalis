import json
from dataclasses import dataclass, field
from typing import List
from busline.event.event import Event
from abc import ABC, abstractmethod


@dataclass(frozen=True)
class OrbEvent(Event, ABC):
    """

    Author: Nicola Ricciardi
    """
