from typing import Optional



class WellKnownTopic:
    """


    Author: Nicola Ricciardi
    """

    _all_topic_wildcard: str = "*"

    @classmethod
    def discover_topic(cls) -> str:
        """
        CORE ---discover---> PLUGIN
        """

        return "$handshake.discover"


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

    @classmethod
    def build_keepalive_topic(cls, identifier: str) -> str:
        """
        TODO
        """

        return f"$keepalive.{identifier}"

    @classmethod
    def build_general_purpose_use_topic(cls, identifier: str) -> str:
        """
        TODO
        """

        return f"$hook.{identifier}"
