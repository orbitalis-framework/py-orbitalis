from typing import Optional



class WellKnownHandShakeTopic:
    """


    Author: Nicola Ricciardi
    """

    _all_topic_wildcard: str = "*"

    """
    CORE ---discover---> PLUGIN
    """
    DISCOVER_TOPIC = "$handshake.discover"


    @classmethod
    def build_offer_topic(cls, core_identifier: str) -> str:
        """
        CORE <---offer--- PLUGIN
        """

        return f"$handshake.{core_identifier}.offer"


    @classmethod
    def build_reply_topic(cls, core_identifier: str, plugin_identifier: str) -> str:
        """
        CORE ---reply---> PLUGIN
        """

        return f"$handshake.{core_identifier}.{plugin_identifier}.reply"


    @classmethod
    def build_response_topic(cls, core_identifier: str, plugin_identifier: str) -> str:
        """
        CORE <---response--- PLUGIN
        """

        return f"$handshake.{core_identifier}.{plugin_identifier}.response"


    # PLUG_OFFER = Topic("$plug-offer", description="CORE <---offer--- PLUGIN", content_type="application/json")
    # PLUG_REPLY = Topic("$plug-reply", description="CORE ---reply---> PLUGIN", content_type="application/json")
    # PLUG_OUTCOME = Topic("$plug-outcome", description="CORE <---outcome--- PLUGIN", content_type="application/json")
    #
    # # === OTHERS ===
    # KEEP_ALIVE = Topic("$keep-alive", description="PLUGIN ---keep alive---> CORE", content_type="application/json")
    # UNPLUG = Topic("$unplug", description="PLUGIN <---unplug---> CORE", content_type="application/json")
