from dataclasses import dataclass, field
from typing import Dict, Optional, Set

from busline.client.subscriber.event_handler.event_handler import EventHandler


@dataclass
class Feature:
    input_message_schema: Dict
    output_message_schema: Dict




