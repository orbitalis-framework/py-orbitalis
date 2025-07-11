import functools
import inspect
from dataclasses import dataclass, field
from typing import Optional, Dict, Awaitable, Callable, List, Type

from busline.client.subscriber.topic_subscriber.event_handler import event_handler
from busline.client.subscriber.topic_subscriber.event_handler.event_handler import EventHandler
from busline.client.subscriber.topic_subscriber.event_handler.schemafull_handler import SchemafullEventHandler
from busline.event.avro_payload import AvroEventPayload
from busline.event.event import Event
from orbitalis.utils.allowblocklist import AllowBlockListMixin


@dataclass(frozen=True)
class Hook:
    """
    Handle plugins operation result messages or unrelated messages

    Author: Nicola Ricciardi
    """

    hook_name: str
    handler: EventHandler
    input_schemas: List[str]
    related_operations: List[str] = field(default_factory=list)
