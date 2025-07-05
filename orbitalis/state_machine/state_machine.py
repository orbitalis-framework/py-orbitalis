from __future__ import annotations
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
import logging
from typing import override, Self


class StateMachine(ABC):
    __state: State = None

    @property
    def state(self):
        return self.__state

    @state.setter
    def state(self, s: State):
        logging.info(f"{self}: {self.__state.name if self.__state is not None else 'None'} --> {s.name}")
        self.__state = s


@dataclass
class PreventInitStateMixin(ABC):
    _allow_init: bool = field(default=False, init=False)

    def __post_init__(self):
        if not self._allow_init:
            raise NotImplemented("Canonical constructor is not available for this class, use 'create' instead")


    @classmethod
    async def create(cls, context: StateMachine, *args, **kwargs) -> Self:
        cls._allow_init = True
        state = await cls._internal_create(context, *args, **kwargs)
        cls._allow_init = False
        return state

    @classmethod
    @abstractmethod
    async def _internal_create(cls, context: StateMachine, *args, **kwargs) -> Self:
        raise NotImplemented()


@dataclass
class State(ABC):

    context: StateMachine
    name: str

    @abstractmethod
    async def handle(self, *args, **kwargs):
        raise NotImplemented()

