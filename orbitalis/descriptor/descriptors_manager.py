from dataclasses import dataclass, field
from typing import Dict
from orbitalis.descriptor.descriptor import Descriptor


@dataclass
class DescriptorsManager:

    descriptors_by_identifier: Dict[str, Descriptor] = field(default_factory=dict)


    def add_descriptor(self, descriptor: Descriptor):
        self.descriptors_by_identifier[descriptor.identifier] = descriptor

    def remove_descriptor(self, descriptor: Descriptor):
        del self.descriptors_by_identifier[descriptor.identifier]

    def remove_descriptor_by_identifier(self, identifier: str):
        del self.descriptors_by_identifier[identifier]
