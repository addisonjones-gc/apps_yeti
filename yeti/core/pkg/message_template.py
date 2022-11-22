# pylint: disable=no-name-in-module
# pylint: disable=no-member
from collections import OrderedDict
from typing import List

from google.cloud.bigquery_storage_v1beta2 import types
from google.protobuf import descriptor_pb2
from google.protobuf.descriptor_pb2 import FieldDescriptorProto
from google.protobuf.proto_builder import MakeSimpleProtoClass

BIGQUERY_TO_PROTO_MAP = {
    "STRING": FieldDescriptorProto.TYPE_STRING,
    "INT64": FieldDescriptorProto.TYPE_INT64,
    "FLOAT": FieldDescriptorProto.TYPE_FLOAT,
    "TIMESTAMP": FieldDescriptorProto.TYPE_STRING,
}


class StreamMessageTemplate:
    """
    A class that generates a complete definition for messages sent via a BigQuery
    write stream
    """

    def __init__(self, json_schema: dict) -> None:
        self.message_class = self._create_simple_proto_class(json_schema)
        self.message_schema = self._create_table_field_schema_from_message()
        self.proto_schema = self._create_proto_schema_from_message()

    # def _create_simple_proto_class(self, field_names: List):
    def _create_simple_proto_class(self, json_schema: dict):
        """Generates simple proto class with explicitly string cast datatypes

        :param field_names: A JSON schema to create the fields and datatypes
                            of the dynamic protobuf class. Format outlined in the
                            YetiRequest type docstring
        :type field_names: dict
        :return: SimpleProtoClass
        :rtype: ProtoClass
        """
        proto_fields = OrderedDict(
            {
                # If datatype not found in map, set to TYPE_STRING
                # by default
                field_name: BIGQUERY_TO_PROTO_MAP.get(
                    datatype, FieldDescriptorProto.TYPE_STRING
                )
                for field_name, datatype in json_schema.items()
            }
        )

        # if "_PARTITIONTIME" not in proto_fields:
        #     proto_fields.update({"_PARTITIONTIME": FieldDescriptorProto.TYPE_STRING})

        return MakeSimpleProtoClass(
            fields=proto_fields,
            full_name="yeti.DynamicClass",
        )

    def _create_proto_schema_from_message(self):
        descriptor_fields = self.message_class.DESCRIPTOR.fields
        field_proto_list = []
        for i, field in enumerate(descriptor_fields):
            field_proto_list.append(
                descriptor_pb2.FieldDescriptorProto(
                    name=field.name, number=i + 1, type=field.type
                )
            )

        proto_schema = types.ProtoSchema()
        proto_schema.proto_descriptor = descriptor_pb2.DescriptorProto(
            name="DynamicMessage", field=field_proto_list
        )
        return proto_schema

    def _create_table_field_schema_from_message(self):
        descriptor_fields = self.message_class.DESCRIPTOR.fields
        table_field_schema_list = []
        for field in descriptor_fields:
            table_field_schema_list.append(
                types.TableFieldSchema(
                    name=field.name,
                    type_=types.TableFieldSchema.Type.STRING,
                )
            )

        return types.TableSchema(fields=table_field_schema_list)

    def json_to_serialized_data(self, input_json: List[dict]) -> bytes:
        """Method to convert JSON data into byte serialized data for a
           protobuf message

        :param input_json: JSON serializable List
        :type input_json_str: List
        :return: Returns input_json as serialized bytes in protobuf format
        :rtype: bytes
        """
        # Handle if single row of JSON is passed

        message_obj = self.message_class()
        serialized_data = []
        for row_dict in input_json:
            for key, val in row_dict.items():
                setattr(message_obj, key, val)
            message_str = message_obj.SerializeToString()
            serialized_data.append(message_str)

        return serialized_data
