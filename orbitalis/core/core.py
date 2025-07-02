from abc import ABC
from collections import defaultdict
from dataclasses import dataclass, field
from typing import override, Dict, Set, Optional

from orbitalis.core.configuration import CoreConfiguration
from orbitalis.core.core_service import CoreServiceDescriptor, CoreServiceNeed
from orbitalis.core.descriptor import CoreDescriptor
from orbitalis.core.handler.handshake import OfferHandler, ResponseHandler
from orbitalis.core.plug.pending import PluginPendingRequest
from orbitalis.core.plugin_descriptor_manager import PluginDescriptorsManager
from orbitalis.events.handshake.discover import DiscoverMessage
from orbitalis.events.wellknown_topic import WellKnownHandShakeTopic
from orbitalis.orb.orb import Orb
import copy

from orbitalis.plugin.descriptor import PluginDescriptor


@dataclass(kw_only=True)
class Core(Orb, ABC):
    services: Dict[str, CoreServiceDescriptor]      # service_name => service
    configuration: CoreConfiguration = field(default_factory=CoreConfiguration)
    plugins_by_service: Dict[str, PluginDescriptorsManager] = field(default_factory=lambda: defaultdict(PluginDescriptorsManager))       # service_name => PluginDescriptor
    pending_requests: Set[PluginPendingRequest] = field(default_factory=set)

    def __post_init__(self):
        self._offer_handler = OfferHandler(self)
        self._response_handler = ResponseHandler(self)

        for _, service_descriptor in self.services.items():
            service_descriptor.service.context = self
            service_descriptor.service.refresh_context()

    @property
    def offer_handler(self) -> OfferHandler:
        return self._offer_handler

    @property
    def response_handler(self) -> ResponseHandler:
        return self._response_handler

    def _needs_for_service(self, service_name: str, service_descriptor: CoreServiceDescriptor) -> Optional[CoreServiceNeed]:
        """

        :return: None is there are no needs
        """

        if service_descriptor.need is None:
            return None

        plugins: PluginDescriptorsManager = self.plugins_by_service[service_name]

        needs: CoreServiceNeed = copy.deepcopy(service_descriptor.need)

        needs.mandatory_plugins_by_identifier = needs.mandatory_plugins_by_identifier.difference(plugins.descriptors_by_identifier)
        needs.optional_plugins_by_identifier = needs.mandatory_plugins_by_identifier.difference(plugins.descriptors_by_identifier)

        for plugins_by_tag, reference in zip([needs.mandatory_plugins_by_category, needs.optional_plugins_by_category], [needs.mandatory_plugins_by_category, needs.optional_plugins_by_category]):
            for tag, need in plugins_by_tag.items():
                n_plugins = len(plugins.descriptors_by_tag[tag])

                need.maximum -= max(0, need.maximum - n_plugins)

                if need.maximum == 0:
                    del reference[tag]

                need.minimum = max(0, need.minimum - n_plugins)

        return needs

    def _is_plugin_needed_and_pluggable_for_service(self, service_name: str, service_descriptor: CoreServiceDescriptor, plugin_descriptor: PluginDescriptor) -> bool:
        need: CoreServiceNeed = self._needs_for_service(service_name, service_descriptor)

        if not need.something_needed():
            return False

        return need.is_plugin_pluggable(plugin_descriptor)

    def is_plugin_needed_and_pluggable(self, plugin_descriptor: PluginDescriptor) -> bool:
        for service_name, service_descriptor in self.services:
            pluggable: bool = self._is_plugin_needed_and_pluggable_for_service(service_name, service_descriptor, plugin_descriptor)

            if not pluggable:
                return False

        return True

    def plug(self, service_name: str, plugin_descriptor: PluginDescriptor):
        """
        Plug given plugin. Raise an exception if service name is invalid.
        """

        if service_name not in self.services:
            raise ValueError(f"'{service_name}' is an invalid service name")

        self.plugins_by_service[service_name].add_descriptor(plugin_descriptor)

    def unplug(self, service_name: str, plugin_identifier: str):
        """
        Unplug given plugin. Raise an exception if service name is invalid.
        """

        if service_name not in self.services:
            raise ValueError(f"'{service_name}' is an invalid service name")

        self.plugins_by_service[service_name].remove_descriptor_by_identifier(plugin_identifier)

    def _is_compliance_for_service(self, service_name: str, service_descriptor: CoreServiceDescriptor) -> bool:
        """
        Return True if the plugged plugins are enough to perform given service
        """

        needs = self._needs_for_service(service_name, service_descriptor)

        if needs is None:
            return True

        return not needs.something_needed()

    def is_compliance(self) -> bool:
        """
        Return True if the plugged plugins are enough to perform all services
        """

        for service_name, service_descriptor in self.services.items():
            if not self._is_compliance_for_service(service_name, service_descriptor):
                return False

        return True


    async def send_discover(self) -> None:

        needs = {}
        for service_name, service_descriptor in self.services.items():
            needs[service_name] = self._needs_for_service(service_name, service_descriptor)

        offer_topic: str = WellKnownHandShakeTopic.build_offer_topic(self.identifier)

        await self.eventbus_client.subscribe(
            topic=offer_topic,
            handler=self._offer_handler
        )

        await self.eventbus_client.publish(
            WellKnownHandShakeTopic.DISCOVER_TOPIC,
            DiscoverMessage(
                core_descriptor=self.generate_descriptor(),
                needs=needs,
                offer_topic=offer_topic
            ).into_event()
        )



    @override
    async def _internal_start(self, *args, **kwargs):
        await super()._internal_start(*args, **kwargs)

        # TODO: self.set_state(NonCompliance())

    @override
    def generate_descriptor(self) -> CoreDescriptor:
        return CoreDescriptor(
            identifier=self.identifier,
            services=dict([(service_name, service_descriptor.service.generate_features_description()) for service_name, service_descriptor in self.services.items()])
        )

