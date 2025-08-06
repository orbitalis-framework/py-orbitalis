import json
from dataclasses import dataclass, field
from typing import List, Type, Self, override

from busline.event.message.number_message import Int64Message, Int32Message, Float64Message, Float32Message
from busline.event.message.string_message import StringMessage
from dataclasses_avroschema import AvroModel

from busline.event.message.avro_message import AvroMessageMixin


@dataclass(kw_only=True)
class SchemaSpec(AvroModel):
    """

    Author: Nicola Ricciardi
    """

    schemas: List[str] = field(default_factory=list)
    support_empty_schema: bool = field(default=False)
    support_undefined_schema: bool = field(default=False)

    def with_empty_support(self) -> Self:
        self.support_empty_schema = True
        return self

    @property
    def has_some_explicit_schemas(self) -> bool:
        return len(self.schemas) > 0

    @classmethod
    def from_schema(cls, schema: str) -> Self:
        return cls(schemas=[schema])

    @classmethod
    def empty(cls) -> Self:
        return cls(support_empty_schema=True)

    @classmethod
    def undefined(cls) -> Self:
        return cls(schemas=[], support_empty_schema=False)

    @classmethod
    def from_message(cls, payload: Type[AvroMessageMixin]) -> Self:
        return cls.from_schema(payload.avro_schema())

    @classmethod
    def int64(cls) -> Self:
        return cls.from_message(Int64Message)

    @classmethod
    def int32(cls) -> Self:
        return cls.from_message(Int32Message)

    @classmethod
    def float64(cls) -> Self:
        return cls.from_message(Float64Message)

    @classmethod
    def float32(cls) -> Self:
        return cls.from_message(Float32Message)

    @classmethod
    def string(cls) -> Self:
        return cls.from_message(StringMessage)


    def is_compatible(self, other: Self) -> bool:
        if self.support_undefined_schema != other.support_undefined_schema:
            return False

        if self.support_empty_schema != other.support_empty_schema:
            return False

        if len(self.schemas) != len(other.schemas):
            return False

        for my_schema in self.schemas:
            found = False

            for other_schema in other.schemas:
                if self._compare_two_schema(my_schema, other_schema):
                    found = True
                    break

            if not found:
                return False

        return True

    def is_compatible_with_schema(self, target_schema: str) -> bool:
        if self.support_undefined_schema:
            return True

        for my_schema in self.schemas:
            if self._compare_two_schema(my_schema, target_schema):
                return True

        return False


    @classmethod
    def _compare_two_schema(cls, schema_a: str, schema_b: str):
        """
        Compare two schemas and return True if they are equal
        """

        try:
            schema_a_dict = json.loads(schema_a)
            schema_b_dict = json.loads(schema_b)

            return schema_a_dict == schema_b_dict
        except:
            return schema_a == schema_b


@dataclass
class Input(SchemaSpec):

    @property
    def has_input(self) -> Self:
        return self.support_undefined_schema or self.support_empty_schema or self.has_some_explicit_schemas

    @classmethod
    def no_input(cls) -> Self:
        return cls()

    @override
    def is_compatible(self, other: Self) -> bool:
        if int(other.has_input) + int(self.has_input) == 1:
            return False

        return super().is_compatible(other)

@dataclass(kw_only=True)
class Output(SchemaSpec):

    @property
    def has_output(self) -> Self:
        return self.support_undefined_schema or self.support_empty_schema or self.has_some_explicit_schemas

    @classmethod
    def no_output(cls) -> Self:
        return cls()

    @override
    def is_compatible(self, other: Self) -> bool:
        if int(other.has_output) + int(self.has_output) == 1:
            return False

        return super().is_compatible(other)


@dataclass(kw_only=True)
class Inputs(AvroModel):
    inputs: List[Input]

    def input_is_compatible(self, input: Input) -> bool:
        found = False
        for my_input in self.inputs:

            if my_input.is_compatible(input):
                found = True
                break

        return found


@dataclass(kw_only=True)
class Outputs(AvroModel):
    outputs: List[Output]

    def output_is_compatible(self, output: Output) -> bool:
        found = False
        for my_output in self.outputs:

            if my_output.is_compatible(output):
                found = True
                break

        return found
