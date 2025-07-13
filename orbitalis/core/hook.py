import functools
import inspect
from dataclasses import dataclass, field
from typing import Optional, Dict, Awaitable, Callable, List, Type

from busline.client.subscriber.topic_subscriber.event_handler import event_handler
from busline.client.subscriber.topic_subscriber.event_handler.event_handler import EventHandler
from busline.client.subscriber.topic_subscriber.event_handler.schemafull_handler import SchemafullEventHandler
from busline.event.avro_payload import AvroEventPayload
from busline.event.event import Event
from orbitalis.orbiter.schemaspec import SchemaSpec
from orbitalis.utils.allowblocklist import AllowBlockListMixin


@dataclass(frozen=True)
class Hook:
    """
    Handle plugins operation result messages or unrelated messages

    Author: Nicola Ricciardi
    """

    hook_name: str
    handler: EventHandler
    related_operations: List[str]


@dataclass(kw_only=True)
class HooksProviderMixin:

    hooks: Dict[str, Hook] = field(default_factory=dict, init=False)    # hook_name => Hook

    def __post_init__(self):
        # used to refresh hooks
        for attr_name in dir(self):
            _ = getattr(self, attr_name)