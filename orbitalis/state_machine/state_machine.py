from __future__ import annotations
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
import logging
from typing import override, Self


class StateMachine(ABC):
    __state: State = None

    def __init__(self):
        self.__state = Created(self)

    @property
    def state(self):
        return self.__state

    @state.setter
    def state(self, s: State):
        self.set_state(s)

    def set_state(self, s: State):
        logging.info(f"{self}: {self.__state.name} --> {s.name}")
        self.__state = s

    async def start(self, *args, **kwargs):
        logging.info(f"{self}: starting...")
        await self.on_starting(*args, **kwargs)
        await self._internal_start(*args, **kwargs)
        await self.on_started(*args, **kwargs)
        logging.info(f"{self}: started")


    async def on_starting(self, *args, **kwargs):
        """
        TODO
        """

    @abstractmethod
    async def _internal_start(self, *args, **kwargs):
        """
        TODO
        """

    async def on_started(self, *args, **kwargs):
        """
        TODO
        """

    async def stop(self, *args, **kwargs):
        logging.info(f"{self}: stopping...")
        await self.on_stopping(*args, **kwargs)
        await self._internal_stop(*args, **kwargs)
        await self.on_stopped(*args, **kwargs)
        logging.info(f"{self}: stopped")


    async def on_stopping(self, *args, **kwargs):
        """
        TODO
        """

    async def _internal_stop(self, *args, **kwargs):
        self.state = Stopped(self)

    async def on_stopped(self, *args, **kwargs):
        """
        TODO
        """


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



@dataclass
class Created(State):

    name: str = field(default="CREATED", init=False)

    @override
    async def handle(self, *args, **kwargs):
        raise NotImplemented(f"{self.name} state is able to handle *nothing*")


@dataclass
class Stopped(State):

    name: str = field(default="STOPPED", init=False)

    @override
    async def handle(self, *args, **kwargs):
        raise NotImplemented(f"{self.name} state is able to handle *nothing*")
