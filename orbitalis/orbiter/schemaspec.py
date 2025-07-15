import json
from dataclasses import dataclass, field
from typing import Optional, List, Type, Self
from abc import abstractmethod
from busline.event.avro_payload import AvroEventPayload


@dataclass(kw_only=True)
class SchemaSpec:
    """

    Author: Nicola Ricciardi
    """

    schemas: List[str] = field(default_factory=list)
    empty_schema: bool = field(default=False)

    @property
    def support_empty_schema(self) -> bool:
        return self.empty_schema

    @property
    def has_some_explicit_schemas(self) -> bool:
        return len(self.schemas) > 0

    @property
    def is_undefined(self) -> bool:
        return not self.has_some_explicit_schemas and not self.empty_schema

    @classmethod
    def from_schema(cls, schema: str) -> Self:
        return cls(schemas=[schema])

    @classmethod
    def empty(cls) -> Self:
        return cls(empty_schema=True)

    @classmethod
    def undefined(cls) -> Self:
        return cls(schemas=[], empty_schema=False)

    @classmethod
    def from_payload(cls, payload: Type[AvroEventPayload]) -> Self:
        return cls.from_schema(payload.avro_schema())


    def is_compatible(self, other: Self, *, undefined_is_compatible: bool = False, strict: bool = False) -> bool:
        if self.is_undefined and other.is_undefined:
            return True

        if not undefined_is_compatible:
            if (not self.is_undefined and other.is_undefined) or (self.is_undefined and not other.is_undefined):
                return False
        else:
            if self.is_undefined or other.is_undefined:
                return True

        if (self.empty_schema and not other.empty_schema) or (not self.empty_schema and other.empty_schema):
            return False

        if (self.has_some_explicit_schemas and not other.has_some_explicit_schemas) or (not self.has_some_explicit_schemas and other.has_some_explicit_schemas):
            return False

        for my_schema in self.schemas:
            found = False

            for other_schema in other.schemas:
                if self.compare_two_schema(my_schema, other_schema):
                    found = True

            if not found:
                return False

        if strict:
            for other_schema in other.schemas:
                found = False

                for my_schema in self.schemas:
                    if self.compare_two_schema(other_schema, my_schema):
                        found = True

                if not found:
                    return False

        return True

    def is_compatible_with_schema(self, target_schema: str, undefined_is_compatible: bool = False) -> bool:
        if undefined_is_compatible and self.is_undefined:
            return True

        for my_schema in self.schemas:
            if self.compare_two_schema(my_schema, target_schema):
                return True

        return False


    @classmethod
    def compare_two_schema(cls, schema_a: str, schema_b: str):
        """
        Compare two schemas and return True if they are equal
        """

        try:
            schema_a_dict = json.loads(schema_a)
            schema_b_dict = json.loads(schema_b)

            return schema_a_dict == schema_b_dict
        except:
            return schema_a == schema_b


@dataclass(kw_only=True)
class InputOutputSchemaSpec:
    input: Optional[SchemaSpec]
    output: Optional[SchemaSpec] = field(default=None)

    @property
    def has_input(self) -> bool:
        return self.input is not None

    @property
    def has_output(self) -> bool:
        return self.output is not None
