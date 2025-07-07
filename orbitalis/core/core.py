from datetime import datetime
import logging
from abc import ABC
from collections import defaultdict
from dataclasses import dataclass, field
from typing import override, Dict, Set, Optional

from dataclasses_avroschema import AvroModel

from busline.client.subscriber.topic_subscriber.event_handler import schemafull_event_handler
from busline.event.event import Event
from orbitalis.core.configuration import CoreConfiguration, Need
from orbitalis.core.core_service import CoreServiceDescriptor, CoreServiceNeed
from orbitalis.core.descriptor import CoreDescriptor
from orbitalis.core.plugin_descriptor_manager import PluginDescriptorsManager
from orbitalis.core.state import CoreState
from orbitalis.events.handshake.discover import DiscoverMessage
from orbitalis.events.handshake.offer import OfferMessage
from orbitalis.events.handshake.reply import ReplyMessage
from orbitalis.events.handshake.response import ResponseMessage
from orbitalis.events.wellknown_topic import WellKnownHandShakeTopic
from orbitalis.orb.orb import Orb

from orbitalis.plugin.descriptor import PluginDescriptor
from orbitalis.state_machine.state_machine import StateMachine


@dataclass(kw_only=True)
class Core(Orb[PluginDescriptor], StateMachine[CoreState]):
    configuration: CoreConfiguration = field(default_factory=CoreConfiguration)

    def __post_init__(self):
        self.state = CoreState.CREATED


    def _need_for_operation(self, operation_name: str) -> Optional[Need]:
        """

        :return: None is there are no needs
        """

        if operation_name not in self.configuration.needs.keys():
            return None

        need = self.configuration.needs[operation_name].to_need()

        for plugin_identifier, connection in self.remote_operations[operation_name].items():
            need.mandatory.discard(plugin_identifier)

            if need.maximum is not None:
                need.maximum = max(0, need.maximum - 1)

            need.minimum = max(0, need.minimum - 1)

        if need.maximum is not None and need.maximum == 0:
            return None

        return need


    def _is_plugin_needed_and_pluggable_for_service(self, service_name: str, service_descriptor: CoreServiceDescriptor, plugin_descriptor: PluginDescriptor) -> bool:
        """
        TODO
        """

    def is_plugin_needed_and_pluggable(self, plugin_descriptor: PluginDescriptor) -> bool:
        """
        TODO
        """

    def plug(self, service_name: str, plugin_descriptor: PluginDescriptor):
        """
        Plug given plugin. Raise an exception if service name is invalid.
        """

        # TODO

    def unplug(self, service_name: str, plugin_identifier: str):
        """
        Unplug given plugin. Raise an exception if service name is invalid.
        """

        # TODO

    def is_compliance_for_operation(self, operation_name: str) -> bool:
        """
        Return True if the plugged plugins are enough to perform given service
        """

        return self._need_for_operation(operation_name) is None

    def is_compliance(self) -> bool:
        """
        Return True if the plugged plugins are enough to perform all services
        """

        for operation_name in self.configuration.needs.keys():
            if not self.is_compliance_for_operation(operation_name):
                return False

        return True

    def _update_compliance(self):

        if self.is_compliance():
            self.state = CoreState.COMPLIANT
        else:
            self.state = CoreState.NOT_COMPLIANT

    @schemafull_event_handler(AvroModel.avro_schema_to_python(ResponseMessage))
    async def response_event_handler(self, topic: str, event: Event[ResponseMessage]):
        logging.debug(f"{self}: new response: {topic} -> {event}")

    @schemafull_event_handler(AvroModel.avro_schema_to_python(OfferMessage))
    async def offer_event_handler(self, topic: str, event: Event[OfferMessage]):
        logging.debug(f"{self}: new offer: {topic} -> {event}")

        if self.is_plugin_needed_and_pluggable(event.payload.plugin_descriptor):

            response_topic: str = WellKnownHandShakeTopic.build_response_topic(
                self.identifier,
                event.payload.plugin_descriptor.identifier
            )

            await self.eventbus_client.subscribe(
                topic=response_topic,
                handler=self.response_event_handler
            )

            await self.eventbus_client.publish(
                topic=event.payload.reply_topic,
                event=ReplyMessage(
                    core_identifier=self.identifier,
                    plug_request=True,
                    description="I need you",
                    response_topic=response_topic
                ).into_event()
            )

            self.pending_requests[event.payload.plugin_descriptor.identifier] = datetime.now()

        else:
            await self.eventbus_client.publish(
                topic=event.payload.reply_topic,
                event=ReplyMessage(
                    core_identifier=self.identifier,
                    plug_request=False,
                    description="Not needed anymore, sorry",
                ).into_event()
            )

    @property
    def offer_topic(self) -> str:
        return f"$handshake.{self.identifier}.offer"

    async def send_discover(self) -> None:

        self._update_compliance()

        if self.state == CoreState.COMPLIANT:
            return

        needs = {}
        for operation_name in self.remote_operations.keys():
            needs[operation_name] = self._need_for_operation(operation_name)

        await self.eventbus_client.subscribe(
            topic=self.offer_topic,
            handler=self.offer_event_handler
        )

        await self.eventbus_client.publish(
            self.configuration.discover_topic,
            DiscoverMessage(
                core_identifier=self.identifier,
                needs=needs,
                offer_topic=self.offer_topic
            ).into_event()
        )


    @override
    async def _internal_start(self, *args, **kwargs):
        await super()._internal_start(*args, **kwargs)

        self._update_compliance()


