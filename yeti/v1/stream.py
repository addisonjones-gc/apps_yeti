# pylint: disable=no-member
from datetime import datetime
from typing import List
from fastapi import HTTPException
from google.api_core.exceptions import InvalidArgument
from google.cloud.bigquery_storage_v1beta2 import BigQueryWriteClient, types
from yeti.core.pkg.message_template import StreamMessageTemplate

from yeti.core.pkg.utils import get_logger
from yeti.core.constants import TABLE_PARTITION_FIELD_NAME

logger = get_logger()


def stream_json_to_bigquery(
    bq_stream_write_client: BigQueryWriteClient,
    bq_project_id: str,
    bq_dataset_id: str,
    bq_table_id: str,
    json_schema: dict,
    json_dict_list: List[dict],
):
    """Method to stream JSON data into a BigQuery table
       using the Storage Write API

    :param bq_stream_write_client: A previously created BigQuery Stream Write Client
    :type bq_stream_write_client: BigQueryWriteClient
    :param bq_project_id: Target BigQuery Project Id
    :type bq_project_id: str
    :param bq_dataset_id: Target BigQuery Dataset Id
    :type bq_dataset_id: str
    :param bq_table_id: Target BigQuery Table Id
    :type bq_table_id: str
    :param json_schema: A dict containing a "key": "value" pair outlined in the
                        YetiRequest type
    :type json_schema: dict
    :param json_dict_list: A JSON serializable list
    :type json_dict_list: List
    :raises HTTPException: Invalid arg in JSON payload
    """
    write_stream = (
        BigQueryWriteClient.table_path(bq_project_id, bq_dataset_id, bq_table_id)
        + "/_default"
    )

    json_protobuf_template = StreamMessageTemplate(json_schema)
    append_requests = []
    append_request = create_append_request(
        message_template=json_protobuf_template,
        input_json=json_dict_list,
        write_stream=write_stream,
    )
    append_requests.append(append_request)

    logger.info("Sending append request")
    start_time = datetime.utcnow()

    try:
        bq_stream_write_client.append_rows(iter(append_requests))

    except InvalidArgument as invalid_arg_exc:
        # logger.info(invalid_arg_exc.errors)
        logger.info(invalid_arg_exc.args)
        raise HTTPException(500, invalid_arg_exc.args) from invalid_arg_exc
    end_time = datetime.utcnow()

    logger.info("Took %s", end_time - start_time)


def create_append_request(
    message_template: StreamMessageTemplate,
    input_json: List,
    write_stream: str,
) -> types.AppendRowsRequest:
    """Function to generate an AppendRowsRequest passed to the
       BigQuery storage write client

    :param message_template: A StreamMessageTemplate object to serialize the
                             JSON data
    :type message_template: StreamMessageTemplate
    :param input_json_str: A JSON List
    :type input_json_str: List
    :param write_stream: The write_stream string to pass as a requirement of the
                         AppendRowsRequest creation
    :type write_stream: str
    :return: Returns an AppendRowsRequest
    :rtype: types.AppendRowsRequest
    """

    # Serialize input_json to protobuf bytes
    serialized_rows = message_template.json_to_serialized_data(input_json)
    proto_rows = types.ProtoRows()
    proto_rows.serialized_rows.extend(serialized_rows)

    append_request = types.AppendRowsRequest(write_stream=write_stream)

    append_request_data = types.AppendRowsRequest.ProtoData()
    append_request_data.rows = proto_rows
    append_request_data.writer_schema = message_template.proto_schema

    append_request.proto_rows = append_request_data

    return append_request
