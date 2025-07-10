import functools
import inspect
from dataclasses import dataclass, field
from typing import Optional, Dict, Awaitable, Callable, List, Type

from busline.client.subscriber.topic_subscriber.event_handler import schemafull_event_handler
from busline.client.subscriber.topic_subscriber.event_handler.schemafull_handler import SchemafullEventHandler
from busline.event.avro_payload import AvroEventPayload
from busline.event.event import Event
from orbitalis.utils.allowblocklist import AllowBlockPriorityListMixin


@dataclass
class Policy(AllowBlockPriorityListMixin):
    maximum: Optional[int] = field(default=None)


@dataclass(frozen=True)
class Operation:
    handler: SchemafullEventHandler
    policy: Optional[Policy] = field(default=None)


def operation(*, name: Optional[str] = None, policy: Optional[Policy] = None, schemas: List[Dict] | Dict):

    def decorator(method: Callable[[str, Event], Awaitable]):

        if not inspect.iscoroutinefunction(method):
            raise TypeError("Event handler must be an async function or method.")

        method_name = method.__name__
        operation_name = name or method_name

        @schemafull_event_handler(schemas)
        @functools.wraps(method)
        async def wrapper(self, *args, **kwargs):

            if not hasattr(self, "operations"):
                raise AttributeError(f"{self} has no 'operations' attribute")

            if operation_name not in self.operations:
                self.operations[operation_name] = Operation(getattr(self, method_name), policy)

            return await method(self, *args, **kwargs)

        return wrapper

    return decorator