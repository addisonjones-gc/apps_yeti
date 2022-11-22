from time import sleep

from google.cloud.bigquery_storage_v1beta2 import BigQueryWriteClient

from yeti.v1.stream import stream_json_to_bigquery
from yeti.core.pkg.utils import get_logger
from yeti.tests.test_utils import generate_sample_json_schema_and_data

logger = get_logger()
logger.setLevel("DEBUG")


def test_stream_json_to_partitioned_table():
    bq_project_id = "motorefi-analytics-dev"
    bq_dataset_id = "de_integration_tests"
    bq_table_id = "yeti_int_test_stream"

    sample_json_schema, sample_json_dict_list = generate_sample_json_schema_and_data(
        100
    )

    bq_write_client = BigQueryWriteClient()
    logger.info("Beginning stream")
    stream_json_to_bigquery(
        bq_stream_write_client=bq_write_client,
        bq_project_id=bq_project_id,
        bq_dataset_id=bq_dataset_id,
        bq_table_id=bq_table_id,
        json_schema=sample_json_schema,
        json_dict_list=sample_json_dict_list,
    )

    sleep(3)


test_stream_json_to_partitioned_table()
