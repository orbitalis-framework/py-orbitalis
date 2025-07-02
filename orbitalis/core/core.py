from abc import ABC
from dataclasses import dataclass, field
from typing import override, Dict, Set

from orbitalis.core.configuration import CoreConfiguration, Service, ServiceNeed
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
    configuration: CoreConfiguration
    plugins_by_service: Dict[str, PluginDescriptorsManager] = field(default_factory=dict)       # feature.name => PluginDescriptor
    pending_requests: Set[PluginPendingRequest] = field(default_factory=set)

    def __post_init__(self):
        self._offer_handler = OfferHandler(self)
        self._response_handler = ResponseHandler(self)

    @property
    def offer_handler(self) -> OfferHandler:
        return self._offer_handler

    @property
    def response_handler(self) -> ResponseHandler:
        return self._response_handler

    def _needs_for_service(self, service: Service) -> ServiceNeed:
        """

        :param service:
        :return: None is there are no needs
        """

        plugins: PluginDescriptorsManager = self.plugins_by_service[service.name]

        needs: ServiceNeed = copy.deepcopy(service.needs)

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

    def _is_plugin_needed_and_pluggable_for_service(self, service: Service, plugin_descriptor: PluginDescriptor) -> bool:
        need: ServiceNeed = self._needs_for_service(service)

        if not need.something_needed():
            return False

        return need.is_plugin_pluggable(plugin_descriptor)

    def is_plugin_needed_and_pluggable(self, plugin_descriptor: PluginDescriptor) -> bool:
        for service in self.configuration.services:
            pluggable: bool = self._is_plugin_needed_and_pluggable_for_service(service, plugin_descriptor)

            if not pluggable:
                return False

        return True

    def plug(self, service_name: str, plugin_descriptor: PluginDescriptor):
        """
        Plug given plugin. Raise an exception if service name is invalid.
        """

        if not self.configuration.is_valid_service(service_name):
            raise ValueError(f"'{service_name}' is an invalid service name")

        self.plugins_by_service[service_name].add_descriptor(plugin_descriptor)

    def unplug(self, service_name: str, plugin_identifier: str):
        """
        Unplug given plugin. Raise an exception if service name is invalid.
        """

        if not self.configuration.is_valid_service(service_name):
            raise ValueError(f"'{service_name}' is an invalid service name")

        self.plugins_by_service[service_name].remove_descriptor_by_identifier(plugin_identifier)

    def _is_compliance_for_service(self, service: Service) -> bool:
        """
        Return True if the plugged plugins are enough to perform given service
        """

        return not self._needs_for_service(service).something_needed()

    def _is_compliance(self) -> bool:
        """
        Return True if the plugged plugins are enough to perform all services
        """

        for service in self.configuration.mandatory_services:
            if not self._is_compliance_for_service(service):
                return False

        return True


    async def send_discover(self) -> None:

        needs_set = set()
        for service in self.configuration.services:
            needs = self._needs_for_service(service)
            needs_set.add(needs)

        offer_topic: str = WellKnownHandShakeTopic.build_offer_topic(self.identifier)

        await self.eventbus_client.subscribe(
            topic=offer_topic,
            handler=self._offer_handler
        )

        await self.eventbus_client.publish(
            WellKnownHandShakeTopic.DISCOVER_TOPIC,
            DiscoverMessage(
                core_descriptor=self.generate_descriptor(),
                needs=needs_set,
                offer_topic=offer_topic
            ).into_event()
        )



    @override
    async def _internal_start(self, *args, **kwargs):
        await super()._internal_start(*args, **kwargs)

        # TODO: self.set_state(NonCompliance())

    @override
    def generate_descriptor(self) -> CoreDescriptor:
        return CoreDescriptor(identifier=self.identifier)

