from dataclasses import dataclass, field
from typing import List, Dict


@dataclass
class NeededPluginByTypeRecord:
    """
    mandatory: `True` if this record is mandatory for core
    min: minimum number of plugins of this type
    max: maximum number of plugins of this type
    whitelist: admitted plugins (by identifiers)
    blacklist: not admitted plugins (by identifiers)

    Author: Nicola Ricciardi
    """

    mandatory: bool = field(default=True)
    min: int | None = field(default=None)
    max: int | None = field(default=None)
    whitelist: List[str] | None = field(default=None)
    blacklist: List[str] | None = field(default=None)

    def __post_init__(self):
        if self.min < 0 or self.max < 0 or self.min > self.max:
            raise ValueError("minimum and/or maximum value are invalid")




@dataclass
class CoreConfiguration:
    """
    needed_plugins_by_identifier: list of needed plugins
    needed_plugins_by_type: list of needed plugins
    discovering_interval: interval between two discover event sent

    Author: Nicola Ricciardi
    """

    needed_plugins_by_identifier: List[str] = field(default_factory=list)
    needed_plugins_by_type: Dict[str, NeededPluginByTypeRecord] = field(default_factory=dict)
    discovering_interval: int = field(default=2)