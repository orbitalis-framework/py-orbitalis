from __future__ import annotations
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
import logging
from typing import override


@dataclass
class StateMachine(ABC):
    __state: State = field(default=None, init=False)

    def __post_init__(self):
        self.__state = Created(self)

    @property
    def state(self):
        return self.__state

    @state.setter
    def state(self, s: State):
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
class State(ABC):

    context: StateMachine
    name: str

    @abstractmethod
    async def handle(self, *args, **kwargs):
        raise NotImplemented()

    @classmethod
    @abstractmethod
    async def mount(cls, context: StateMachine, *args, **kwargs):
        raise NotImplemented()


@dataclass
class Created(State):

    name: str = field(default="CREATED", init=False)

    @override
    async def handle(self, *args, **kwargs):
        raise NotImplemented(f"{self.name} state is able to handle *nothing*")


    @classmethod
    async def mount(cls, context: StateMachine, *args, **kwargs):
        context.state = Created(context)


@dataclass
class Stopped(State):

    name: str = field(default="STOPPED", init=False)

    @override
    async def handle(self, *args, **kwargs):
        raise NotImplemented(f"{self.name} state is able to handle *nothing*")

    @classmethod
    async def mount(cls, context: StateMachine, *args, **kwargs):
        context.state = Created(context)