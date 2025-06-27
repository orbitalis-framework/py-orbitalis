from typing import Optional



class WellKnownHandShakeTopic:
    """


    Author: Nicola Ricciardi
    """

    _all_topic_wildcard: str = "*"

    @classmethod
    def build_discover_topic(cls, core_identifier: Optional[str] = None) -> str:
        """
        CORE ---discover---> PLUGIN
        """

        return f"$handshake.{core_identifier if core_identifier is not None else cls._all_topic_wildcard}.discover"

    @classmethod
    def build_offer_topic(cls, core_identifier: str) -> str:
        """
        CORE <---offer--- PLUGIN
        """

        return f"$handshake.{core_identifier}.offer"



    # PLUG_OFFER = Topic("$plug-offer", description="CORE <---offer--- PLUGIN", content_type="application/json")
    # PLUG_REPLY = Topic("$plug-reply", description="CORE ---reply---> PLUGIN", content_type="application/json")
    # PLUG_OUTCOME = Topic("$plug-outcome", description="CORE <---outcome--- PLUGIN", content_type="application/json")
    #
    # # === OTHERS ===
    # KEEP_ALIVE = Topic("$keep-alive", description="PLUGIN ---keep alive---> CORE", content_type="application/json")
    # UNPLUG = Topic("$unplug", description="PLUGIN <---unplug---> CORE", content_type="application/json")
