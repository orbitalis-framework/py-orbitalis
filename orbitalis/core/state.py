from enum import Enum


class CoreState(Enum):
    CREATED = 1
    READY = 2
    STARTING = 3
    RUNNING = 4
    STOPPING = 5
    STOPPED = 6