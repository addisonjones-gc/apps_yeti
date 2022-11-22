from time import sleep

from google.cloud.bigquery import Client as BQ_Client
from google.cloud.bigquery import TimePartitioningType

from yeti.v1.load import load_json_to_bigquery
from yeti.core.pkg.utils import get_logger
from yeti.tests.test_utils import generate_sample_json_schema_and_data

logger = get_logger()
logger.setLevel("DEBUG")


def test_create_and_load_partitioned_table():
    bq_project_id = "motorefi-analytics-dev"
    bq_dataset_id = "de_integration_tests"
    bq_table_id = "yeti_int_test_stream"

    table_fullname = f"{bq_project_id}.{bq_dataset_id}.{bq_table_id}"
    bq_client = BQ_Client(project=bq_project_id)
    logger.info("Deleting table %s if it exists", table_fullname)
    bq_client.delete_table(table_fullname, not_found_ok=True)

    sample_json_schema, sample_json_dict_list = generate_sample_json_schema_and_data(
        100
    )

    load_json_to_bigquery(
        bq_project_id=bq_project_id,
        bq_dataset_id=bq_dataset_id,
        bq_table_id=bq_table_id,
        json_schema=sample_json_schema,
        json_dict_list=sample_json_dict_list,
    )

    sleep(3)
    bq_table = bq_client.get_table(table_fullname)
    assert bq_table.partitioning_type == TimePartitioningType.DAY


test_create_and_load_partitioned_table()
