from enum import StrEnum


class CoreState(StrEnum):
    CREATED = "CREATED"
    NOT_COMPLIANT = "NOT_COMPLIANT"
    COMPLIANT = "COMPLIANT"
    STOPPED = "STOPPED"

