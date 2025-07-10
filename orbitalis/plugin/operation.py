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
    has_output: bool
    output_schemas: Optional[List[str]] = field(default=None)
    policy: Policy = field(default=None)


class _OperationDescriptor:
    def __init__(self, func, op_name, has_output: bool, policy, input_schemas, output_schemas):
        self.func = schemafull_event_handler(input_schemas)(func)
        self.op_name = op_name
        self.policy = policy
        self.has_output = has_output
        self.output_schemas = output_schemas

    def __get__(self, instance, owner):
        if instance is None:
            return self

        if self.op_name not in instance.operations:
            instance.operations[self.op_name] = Operation(
                handler=self.func.__get__(instance, owner),
                policy=self.policy,
                output_schemas=self.output_schemas,
                has_output=self.has_output
            )

        return self.func.__get__(instance, owner)


def operation(*, policy: Policy, has_output: bool, input_schemas: List[Dict] | Dict, output_schemas: Optional[List[Dict] | Dict] = None, name: Optional[str] = None):
    def decorator(func):
        if not inspect.iscoroutinefunction(func):
            raise TypeError("Event handler must be async")

        op_name = name or func.__name__
        return _OperationDescriptor(func, op_name, has_output, policy, input_schemas, output_schemas)
    return decorator
