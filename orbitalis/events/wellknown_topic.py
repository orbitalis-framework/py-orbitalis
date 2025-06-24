from typing import Optional

ALL_WILDCARD = "*"      # TODO: spostare in py-busline


class WellKnownHandShakeTopic:

    @classmethod
    def build_discover_topic(cls, core_identifier: Optional[str] = None) -> str:
        return f"$handshake.discover.{core_identifier if core_identifier is not None else ALL_WILDCARD}"



    # PLUG_OFFER = Topic("$plug-offer", description="CORE <---offer--- PLUGIN", content_type="application/json")
    # PLUG_REPLY = Topic("$plug-reply", description="CORE ---reply---> PLUGIN", content_type="application/json")
    # PLUG_OUTCOME = Topic("$plug-outcome", description="CORE <---outcome--- PLUGIN", content_type="application/json")
    #
    # # === OTHERS ===
    # KEEP_ALIVE = Topic("$keep-alive", description="PLUGIN ---keep alive---> CORE", content_type="application/json")
    # UNPLUG = Topic("$unplug", description="PLUGIN <---unplug---> CORE", content_type="application/json")
