from dataclasses import dataclass, field
from typing import Optional
from orbitalis.need.policy import Policy


@dataclass
class PluginConfiguration:
    """

    Author: Nicola Ricciardi
    """

    acceptance_policy: Policy
    