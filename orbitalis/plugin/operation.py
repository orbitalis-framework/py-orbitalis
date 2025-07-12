import functools
import inspect
from dataclasses import dataclass, field
from typing import Optional, Dict, Awaitable, Callable, List, Type, Self

from busline.client.subscriber.topic_subscriber.event_handler import event_handler
from busline.client.subscriber.topic_subscriber.event_handler.event_handler import EventHandler
from busline.client.subscriber.topic_subscriber.event_handler.schemafull_handler import SchemafullEventHandler
from busline.event.avro_payload import AvroEventPayload
from busline.event.event import Event
from orbitalis.utils.allowblocklist import AllowBlockListMixin


@dataclass
class Policy(AllowBlockListMixin):
    maximum: Optional[int] = field(default=None)


@dataclass(kw_only=True)
class OperationSchema:
    schemas: Optional[List[str]] = field(default=None)
    undefined_schema: bool = field(default=False)
    empty_schema: bool = field(default=False)

    @classmethod
    def from_schema(cls, schema: str) -> Self:
        return OperationSchema(schemas=[schema])

    @classmethod
    def empty(cls) -> Self:
        return OperationSchema(empty_schema=True)

    @classmethod
    def undefined(cls) -> Self:
        return OperationSchema(undefined_schema=True)

    @classmethod
    def from_payload(cls, payload: Type[AvroEventPayload]) -> Self:
        return cls.from_schema(payload.avro_schema())

    def __post_init__(self):
        if len(self.schemas) == 0 and not self.undefined_schema and not self.empty_schema:
            raise ValueError("missed schemas")


@dataclass(kw_only=True)
class Operation:
    name: str
    handler: EventHandler
    policy: Policy
    input: OperationSchema
    output: Optional[OperationSchema]


@dataclass(kw_only=True)
class _OperationDescriptor:
    operation_name: str
    func: Callable[[str, Event], Awaitable]
    policy: Policy
    input: OperationSchema
    output: Optional[OperationSchema]

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


def operation(*, policy: Policy, input: OperationSchema, output: Optional[OperationSchema] = None, name: Optional[str] = None):

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
