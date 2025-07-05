from dataclasses import dataclass, field
from typing import override, TYPE_CHECKING, Self, Optional
from abc import ABC

if TYPE_CHECKING:
    from orbitalis.core.core import Core


@dataclass
class OfCoreMixin(ABC):

    context: Optional['Core'] = field(default=None, repr=False)

    @classmethod
    def from_plugin(cls, core: 'Core') -> Self:
        return cls(core)