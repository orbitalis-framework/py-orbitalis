import inspect
from dataclasses import dataclass, field
from typing import Dict, Self, Any

from busline.client.subscriber.event_handler import event_handler
from busline.client.subscriber.event_handler.event_handler import EventHandler


@dataclass(kw_only=True)
class _SinkDescriptor:
    operation_name: str
    func: Any

    def __post_init__(self):
        self.func = event_handler(self.func)

    def __get__(self, instance, owner):
        if instance is None:
            return self

        if self.operation_name not in instance.operation_sinks:
            instance.operation_sinks[self.operation_name] = self.func.__get__(instance, owner)

        return self.func.__get__(instance, owner)


def sink(operation_name: str):

    def decorator(func):
        if not inspect.iscoroutinefunction(func):
            raise TypeError("Event handler must be async")

        return _SinkDescriptor(
            func=func,
            operation_name=operation_name
        )

    return decorator




@dataclass(kw_only=True)
class SinksProviderMixin:

    operation_sinks: Dict[str, EventHandler] = field(default_factory=dict, init=False)    # operation_name => EventHandler

    def __post_init__(self):
        # used to refresh sinks
        for attr_name in dir(self):
            _ = getattr(self, attr_name)

    def with_operation_sink(self, operation_name: str, handler: EventHandler) -> Self:

        self.operation_sinks[operation_name] = handler

        return self
