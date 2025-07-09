import functools
import inspect
from dataclasses import dataclass, field
from typing import Optional, Dict, Awaitable, Callable
from busline.client.subscriber.topic_subscriber.event_handler.schemafull_handler import SchemafullEventHandler
from orbitalis.utils.allowblocklist import AllowBlockPriorityListMixin


@dataclass
class Policy(AllowBlockPriorityListMixin):
    maximum: Optional[int] = field(default=None)


@dataclass
class Operation:
    handler: SchemafullEventHandler
    policy: Optional[Policy] = field(default=None)
    associated_cores: Dict[str, Optional[str]] = field(default_factory=dict, init=False)  # core_identifier => result_topic


def operation(func: Optional[Callable[..., Awaitable]] = None, /, *, name: Optional[str] = None, policy: Optional[Policy] = None):
    def decorator(method: Callable[..., Awaitable]):
        if not inspect.iscoroutinefunction(method):
            raise TypeError("Event handler must be an async function or method.")

        method_name = method.__name__
        op_name = name or method_name

        @functools.wraps(method)
        async def wrapper(self, *args, **kwargs):
            if not hasattr(self, "operations"):
                raise AttributeError(f"{self} has no 'operations' attribute")

            if op_name not in self.operations:
                self.operations[op_name] = method.__get__(self)

            return await method(self, *args, **kwargs)

        return wrapper

    if func is None:
        return decorator
    else:
        return decorator(func)
