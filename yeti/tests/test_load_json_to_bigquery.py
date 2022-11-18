import json
from time import sleep

from google.cloud.bigquery import Client as BQ_Client
from yeti.v1.load import load_json_to_bigquery
from google.cloud.bigquery import TimePartitioningType

from yeti.core.pkg.utils import get_logger

logger = get_logger()
logger.setLevel("DEBUG")


def test_create_and_load_partitioned_table():
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

    table_fullname = f"{bq_project_id}.{bq_dataset_id}.{bq_table_id}"
    bq_client = BQ_Client(project=bq_project_id)
    logger.info("Deleting table %s if it exists", table_fullname)
    bq_client.delete_table(table_fullname, not_found_ok=True)

    with open("10k_sample.json", mode="r", encoding="utf-8") as f:
        sample_json_str = json.dumps(json.load(f))

    load_json_to_bigquery(
        bq_project_id=bq_project_id,
        bq_dataset_id=bq_dataset_id,
        bq_table_id=bq_table_id,
        json_schema=json_schema,
        json_data=sample_json_str,
    )

    sleep(3)
    bq_table = bq_client.get_table(table_fullname)
    assert bq_table.partitioning_type == TimePartitioningType.DAY


test_create_and_load_partitioned_table()
