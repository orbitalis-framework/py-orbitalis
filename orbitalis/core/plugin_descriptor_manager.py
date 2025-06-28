from dataclasses import dataclass, field
from typing import Dict, Set, Optional
from orbitalis.orb.descriptor.descriptors_manager import DescriptorsManager
from orbitalis.plugin.descriptor import PluginDescriptor


@dataclass
class PluginDescriptorsManager(DescriptorsManager):
    descriptors_by_tag: Dict[str, Set[PluginDescriptor]] = field(default_factory=dict)

    def add_descriptor(self, descriptor: PluginDescriptor):
        self.add_descriptor(descriptor)
        for tag in descriptor.categories:
            self.descriptors_by_tag[tag].add(descriptor)

    def remove_descriptor_by_identifier(self, identifier: str):
        self.remove_descriptor_by_identifier(identifier)
        for tag, descriptors in self.descriptors_by_tag.items():
            target: Optional[PluginDescriptor] = None
            for descriptor in descriptors:
                if descriptor.identifier == identifier:
                    target = descriptor
                    break

            if target is not None:
                descriptors.remove(target)
                break
