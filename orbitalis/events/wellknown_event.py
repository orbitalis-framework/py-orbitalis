from enum import StrEnum


class WellKnownEventType(StrEnum):
    DISCOVER = "handshake-discover"
    OFFER = "handshake-offer"

