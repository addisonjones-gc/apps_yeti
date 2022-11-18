import json
import logging

from fastapi import FastAPI, HTTPException

from google.cloud.bigquery_storage_v1beta2 import BigQueryWriteClient
from google.cloud.bigquery import Client as BQ_Client
from google.api_core.exceptions import NotFound

from yeti.core.models.requests import YetiRequest
from yeti.v1.stream import stream_json_to_bigquery
from yeti.v1.load import load_json_to_bigquery
from yeti.core.pkg.utils import get_logger

logger = get_logger()

HOST = "http://127.0.0.1"
PORT = "8000"

HOSTNAME = f"{HOST}:{PORT}"

bq_stream_write_client = BigQueryWriteClient()
bq_client = BQ_Client()

app = FastAPI()

pending_streams = {}


@app.get("/")
async def root() -> dict:
    """Root basic return function

    :return: Basic hello world message
    :rtype: dict
    """
    return {"message": "Hello World"}


@app.get("/health")
async def health_check() -> dict:
    """Health check endpoint for K8s health checks

    :return: Basic health check
    :rtype: dict
    """
    return {"message": "YETI is up"}


@app.post("/stream_json", status_code=200)
async def stream_json(stream_request: YetiRequest) -> dict:
    """Main POST endpoint to stream JSON data into a BigQuery table
       Takes an HTTP POST request structured in the YetiRequest object
       in core/models/requests.py

    :param stream_request: HTTP POST request definition
    :type stream_request: YetiRequest
    :raises HTTPException: Raises 400 exception on count of new
                           columns > 30% of total columns
    :raises HTTPException: Raises 400 exception if JSON data passed
                           is invalid/not serializable
    :return: Success message for
    :rtype: dict
    """
    bq_client.project = stream_request.bq_project_id
    table_full_path = f"{stream_request.bq_dataset_id}.{stream_request.bq_table_id}"

    table_exists = False
    try:
        bq_table = bq_client.get_table(table=table_full_path)
        table_exists = True
    except NotFound:
        logger.info("Table %s not found, creating", table_full_path)

    if table_exists:
        bq_schema = bq_table.schema
        bq_field_list = [field.name for field in bq_schema]

        new_request_fields = [
            name for name in stream_request.json_schema if name not in bq_field_list
        ]
        new_field_percentage = len(new_request_fields) / len(bq_field_list)
        if new_field_percentage > 1 / 3:
            logger.info(
                "New table percentage for %s is %s",
                table_full_path,
                new_field_percentage,
            )
            raise HTTPException(400, detail="Count of new fields greater than 30%")

        if len(new_request_fields) > 0:
            logger.info("Schema change for table %s detected", table_full_path)
            new_fields_added_string = f"New fields added: {new_request_fields}"
            logger.info(new_fields_added_string)
    else:
        new_request_fields = list(stream_request.json_schema)

    try:
        request_json_str = json.dumps(stream_request.json_data)

    except json.JSONDecodeError as json_decode_error:
        logging.error(json_decode_error)
        raise HTTPException(
            400, detail="Could not deserialize JSON"
        ) from json_decode_error

    if table_exists or len(new_request_fields) == 0:
        stream_json_to_bigquery(
            bq_stream_write_client=bq_stream_write_client,
            bq_project_id=stream_request.bq_project_id,
            bq_dataset_id=stream_request.bq_dataset_id,
            bq_table_id=stream_request.bq_table_id,
            json_schema=stream_request.json_schema,
            json_data=request_json_str,
        )
        upload_method = "STREAM"

    else:
        load_json_to_bigquery(
            bq_project_id=stream_request.bq_project_id,
            bq_dataset_id=stream_request.bq_dataset_id,
            bq_table_id=stream_request.bq_table_id,
            json_schema=stream_request.json_schema,
            json_data=request_json_str,
        )
        upload_method = "LOAD"
        new_fields_added_string = f"New fields added:{new_request_fields}"
    return {
        "message": (
            f"{len(stream_request.json_data)} rows uploaded via {upload_method} to {table_full_path}"
            f"\n{new_fields_added_string if len(new_request_fields) > 0 else ''}"
        )
    }
