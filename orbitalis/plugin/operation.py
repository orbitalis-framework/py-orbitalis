from dataclasses import dataclass, field
from typing import Optional, Dict
from busline.client.subscriber.topic_subscriber.event_handler.schemafull_handler import SchemafullEventHandler
from orbitalis.utils.allowblocklist import AllowBlockPriorityListMixin


@dataclass
class Policy(AllowBlockPriorityListMixin):
    maximum: Optional[int] = field(default=None)


@dataclass
class Operation:
    handler: SchemafullEventHandler
    policy: Optional[Policy] = field(default=None)
    associated_cores: Dict[str, str] = field(default_factory=dict, init=False)  # core_identifier => topic


def operation(handler: Optional[SchemafullEventHandler] = None, /, key: Optional[str] = None, policy: Optional[Policy] = None):
    def decorator(method: SchemafullEventHandler):
        method_name = method.__name__

        class RegisteringDescriptor:
            def __get__(self, instance, owner):
                if instance is None:
                    return self

                handler = getattr(instance, method_name)

                if not hasattr(instance, "operations"):
                    raise AttributeError(f"{instance} has not 'operations'")

                operation_key = key or method_name
                instance.operations[operation_key] = handler

                return handler

        return RegisteringDescriptor()

    if handler is None:
        return decorator
    else:
        return decorator(handler)
