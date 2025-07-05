from dataclasses import dataclass
from typing import override, Dict

from dataclasses_avroschema import AvroModel

from busline.client.subscriber.event_handler.schemafull_handler import SchemafullEventHandler
from busline.event.event import Event
from orbitalis.core.of_core import OfCoreMixin

from orbitalis.core.plug.pending import PluginPendingRequest
from orbitalis.events.handshake.offer import OfferMessage
from orbitalis.events.handshake.reply import ReplyMessage
from orbitalis.events.handshake.response import ResponseMessage
from orbitalis.events.wellknown_topic import WellKnownHandShakeTopic


@dataclass
class OfferHandler(SchemafullEventHandler, OfCoreMixin):


    def input_schema(self) -> Dict:
        return AvroModel.avro_schema_to_python(OfferMessage)

    @override
    async def handle(self, topic: str, event: Event[OfferMessage]):

        if self.context.is_plugin_needed_and_pluggable(event.payload.plugin_descriptor):

            response_topic: str = WellKnownHandShakeTopic.build_response_topic(
                self.context.identifier,
                event.payload.plugin_descriptor.identifier
            )

            await self.context.eventbus_client.subscribe(
                topic=response_topic,
                handler=self.context.response_handler
            )

            await self.context.eventbus_client.publish(
                topic=event.payload.reply_topic,
                event=ReplyMessage(
                    core_identifier=self.context.identifier,
                    plug_request=True,
                    description="I need you",
                    response_topic=response_topic
                ).into_event()
            )

            self.context.pending_requests.add(
                PluginPendingRequest(
                    plugin_descriptor=event.payload.plugin_descriptor,
                    related_topics=set(response_topic)
                )
            )

        else:
            await self.context.eventbus_client.publish(
                topic=event.payload.reply_topic,
                event=ReplyMessage(
                    core_identifier=self.context.identifier,
                    plug_request=False,
                    description="Not needed anymore, sorry",
                ).into_event()
            )

@dataclass
class ResponseHandler(SchemafullEventHandler, OfCoreMixin):

    def input_schema(self) -> Dict:
        return AvroModel.avro_schema_to_python(ResponseMessage)

    @override
    async def handle(self, topic: str, event: Event[ResponseMessage]):
        # TODO
        pass