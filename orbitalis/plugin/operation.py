import functools
import inspect
from dataclasses import dataclass, field
from typing import Optional, Dict, Awaitable, Callable, List, Type, Self, Any

from busline.client.subscriber.topic_subscriber.event_handler import event_handler
from busline.client.subscriber.topic_subscriber.event_handler.event_handler import EventHandler
from busline.event.event import Event
from orbitalis.orbiter.schemaspec import SchemaSpec, InputOutputSchemaSpec
from orbitalis.utils.allowblocklist import AllowBlockListMixin


@dataclass
class Policy(AllowBlockListMixin):
    maximum: Optional[int] = field(default=None)



@dataclass(kw_only=True)
class Operation(InputOutputSchemaSpec):
    name: str
    handler: EventHandler
    policy: Policy


@dataclass(kw_only=True)
class _OperationDescriptor:
    operation_name: str
    func: Any
    policy: Policy
    input: SchemaSpec
    output: Optional[SchemaSpec]

    def __post_init__(self):
        self.func = event_handler(self.func)

    def __get__(self, instance, owner):
        if instance is None:
            return self

        if self.operation_name not in instance.operations:
            instance.operations[self.operation_name] = Operation(
                name=self.operation_name,
                handler=self.func.__get__(instance, owner),
                policy=self.policy,
                input=self.input,
                output=self.output
            )

        return self.func.__get__(instance, owner)


def operation(*, policy: Policy, input: SchemaSpec, output: Optional[SchemaSpec] = None, name: Optional[str] = None):

    def decorator(func):
        if not inspect.iscoroutinefunction(func):
            raise TypeError("Event handler must be async")

        op_name = name or func.__name__

        return _OperationDescriptor(
            func=func,
            operation_name=op_name,
            policy=policy,
            input=input,
            output=output
        )

    return decorator
