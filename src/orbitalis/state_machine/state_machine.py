from __future__ import annotations
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
import logging
from enum import Enum
from typing import TypeVar, Generic



S = TypeVar('S', bound=Enum)


class StateMachine(Generic[S], ABC):
    __state: S = None

    @property
    def state(self):
        return self.__state

    @state.setter
    def state(self, s: S):
        logging.info(f"{self}: {self.__state.name if self.__state is not None else 'None'} --> {s.name}")
        self.__state = s
