import functools
import inspect
from dataclasses import dataclass, field
from typing import Optional, Dict, Awaitable, Callable, List, Type

from busline.client.subscriber.topic_subscriber.event_handler import event_handler
from busline.client.subscriber.topic_subscriber.event_handler.event_handler import EventHandler
from busline.client.subscriber.topic_subscriber.event_handler.schemafull_handler import SchemafullEventHandler
from busline.event.avro_payload import AvroEventPayload
from busline.event.event import Event
from orbitalis.utils.allowblocklist import AllowBlockPriorityListMixin


@dataclass
class Policy(AllowBlockPriorityListMixin):
    maximum: Optional[int] = field(default=None)


@dataclass(frozen=True)
class Operation:
    name: str
    handler: EventHandler
    input_schemas: Optional[List[str]] = field(default=None)
    output_schemas: Optional[List[str]] = field(default=None)
    policy: Policy = field(default=None)


class _OperationDescriptor:
    def __init__(self, func, operation_name, policy, input_schemas, output_schemas):
        self.func = event_handler(func)
        self.operation_name = operation_name
        self.policy = policy
        self.output_schemas = output_schemas
        self.input_schemas = input_schemas

    def __get__(self, instance, owner):
        if instance is None:
            return self

        if self.operation_name not in instance.operations:
            instance.operations[self.operation_name] = Operation(
                handler=self.func.__get__(instance, owner),
                policy=self.policy,
                output_schemas=self.output_schemas,
                input_schemas=self.input_schemas
            )

        return self.func.__get__(instance, owner)


def operation(*, policy: Policy, input_schemas: List[Dict] | Dict, output_schemas: Optional[List[Dict] | Dict] = None, name: Optional[str] = None):
    def decorator(func):
        if not inspect.iscoroutinefunction(func):
            raise TypeError("Event handler must be async")

        op_name = name or func.__name__
        return _OperationDescriptor(func, op_name, policy, input_schemas, output_schemas)
    return decorator
