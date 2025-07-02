import logging
from dataclasses import dataclass
from typing import override, Dict

from dataclasses_avroschema import AvroModel

from busline.client.subscriber.event_handler.schemafull_handler import SchemafullEventHandler
from busline.event.event import Event
from orbitalis.events.handshake.discover import DiscoverMessage
from orbitalis.events.handshake.reply import ReplyMessage
from orbitalis.events.handshake.response import ResponseMessage
from orbitalis.plugin.of_plugin import OfPluginMixin


@dataclass
class DiscoverHandler(SchemafullEventHandler, OfPluginMixin):

    @override
    def input_schema(self) -> Dict:
        return AvroModel.avro_schema_to_python(DiscoverMessage)

    @override
    async def handle(self, topic: str, event: Event[DiscoverMessage]):

        # TODO: use priority to disconnect from a low priority core, if high priority core incomes

        # TODO: use core interface


        if self.context.can_plug_into_core(event.payload.core_descriptor):

            await self.context.send_offer(
                offer_topic=event.payload.offer_topic,
                core_descriptor=event.payload.core_descriptor
            )


@dataclass
class ReplyHandler(SchemafullEventHandler, OfPluginMixin):

    @override
    def input_schema(self) -> Dict:
        return AvroModel.avro_schema_to_python(ReplyMessage)

    @override
    async def handle(self, topic: str, event: Event[ReplyMessage]):

        core_identifier: str = event.payload.core_identifier

        if core_identifier not in self.context.pending_requests:
            logging.debug(f"{self.context}: {core_identifier} sent a ReplyMessage, but there is no related pending request")
            return

        if not event.payload.plug_request:
            logging.debug(f"{self.context}: plug request False; remove {core_identifier} from pending requests")
            self.context.pending_requests.pop(core_identifier, None)
            return

        logging.debug(f"{self.context}: {core_identifier} confirm plug request")

        plug_confirmed: bool = self.context.can_plug_into_core(self.context.pending_requests[core_identifier].core_descriptor)

        logging.debug(f"{self.context}: can plug to {core_identifier}? {plug_confirmed}")

        if plug_confirmed:
            await self.context.plug_into_core(self.context.pending_requests[core_identifier].core_descriptor)

        await self.context.eventbus_client.publish(
            event.payload.response_topic,
            ResponseMessage(
                plugin_identifier=self.context.identifier,
                plug_confirmed=plug_confirmed
            ).into_event()
        )
