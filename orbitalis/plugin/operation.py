import inspect
from abc import ABC
from dataclasses import dataclass, field
from typing import Optional, Dict, Self, Any

from busline.client.subscriber.event_handler import event_handler
from busline.client.subscriber.event_handler.event_handler import EventHandler
from orbitalis.orbiter.schemaspec import Input, Output
from orbitalis.utils.allowblocklist import AllowBlockListMixin


@dataclass
class Policy(AllowBlockListMixin):
    maximum: Optional[int] = field(default=None)

    @classmethod
    def no_constraints(cls) -> Self:
        return cls(maximum=None, allowlist=None, blocklist=None)


@dataclass(kw_only=True)
class Operation:
    name: str
    handler: Optional[EventHandler]
    policy: Policy
    input: Input
    output: Output

    def __post_init__(self):
        if self.input.has_input and self.handler is None:
            raise ValueError("Missed handler")


@dataclass(kw_only=True)
class _OperationDescriptor:
    operation_name: str
    func: Any
    policy: Policy
    input: Input
    output: Output

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


def operation(*, input: Optional[Input] = None, default_policy: Optional[Policy] = None, output: Optional[Output] = None, name: Optional[str] = None):
    """
    Transform a function of a method in an operation and append it to operations provider
    """

    if input is None:
        input = Input.no_input()

    if output is None:
        output = Output.no_output()

    if default_policy is None:
        default_policy = Policy.no_constraints()

    def decorator(func):
        if not inspect.iscoroutinefunction(func):
            raise TypeError("Event handler must be async")

        op_name = name or func.__name__

        return _OperationDescriptor(
            func=func,
            operation_name=op_name,
            policy=default_policy,
            input=input,
            output=output
        )

    return decorator


@dataclass(kw_only=True)
class OperationsProviderMixin(ABC):

    operations: Dict[str, Operation] = field(default_factory=dict)     # operation_name => Operation


    def __post_init__(self):

        # used to refresh operations
        for attr_name in dir(self):
            _ = getattr(self, attr_name)

    def with_operation(self, operation_name: str, operation: Operation) -> Self:
        self.operations[operation_name] = operation

        return self

    def with_custom_policy(self, operation_name: str, policy: Policy) -> Self:
        self.operations[operation_name].policy = policy

        return self
