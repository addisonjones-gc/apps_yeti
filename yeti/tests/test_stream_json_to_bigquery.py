import json
from time import sleep

from google.cloud.bigquery import Client as BQ_Client
from yeti.v1.stream import stream_json_to_bigquery
from google.cloud.bigquery import TimePartitioningType
from google.cloud.bigquery_storage_v1beta2 import BigQueryWriteClient

from yeti.core.pkg.utils import get_logger

logger = get_logger()
logger.setLevel("DEBUG")


def test_stream_json_to_partitioned_table():
    bq_project_id = "motorefi-analytics-dev"
    bq_dataset_id = "addison_sandbox"
    bq_table_id = "yeti_load_create"

    json_schema = {
        "id": "STRING",
        "uuid1": "STRING",
        "uuid2": "STRING",
        "uuid3": "STRING",
        "uuid4": "STRING",
        "uuid5": "STRING",
    }

    with open("10k_sample.json", mode="r", encoding="utf-8") as f:
        sample_json_str = json.dumps(json.load(f))

    bq_write_client = BigQueryWriteClient()
    logger.info("Beginning stream")
    stream_json_to_bigquery(
        bq_stream_write_client=bq_write_client,
        bq_project_id=bq_project_id,
        bq_dataset_id=bq_dataset_id,
        bq_table_id=bq_table_id,
        json_schema=json_schema,
        json_data=sample_json_str,
    )

    sleep(3)


test_stream_json_to_partitioned_table()
