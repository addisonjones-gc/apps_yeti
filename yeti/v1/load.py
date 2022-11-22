import json

from typing import List
from google.cloud.bigquery import (
    Client as BQ_Client,
    Table,
    WriteDisposition,
    LoadJobConfig,
    SchemaUpdateOption,
    SchemaField,
    CreateDisposition,
    TimePartitioning,
    TimePartitioningType,
)

from yeti.core.pkg.utils import get_logger

logger = get_logger()


def load_json_to_bigquery(
    bq_project_id: str,
    bq_dataset_id: str,
    bq_table_id: str,
    json_schema: dict,
    json_dict_list: List[dict],
):
    bq_client = BQ_Client(project=bq_project_id)

    bq_schema = [
        SchemaField(name=field_name, field_type=field_type)
        for field_name, field_type in json_schema.items()
    ]

    job_config = LoadJobConfig(
        write_disposition=WriteDisposition.WRITE_APPEND,
        schema_update_options=[SchemaUpdateOption.ALLOW_FIELD_ADDITION],
        schema=bq_schema,
        create_disposition=CreateDisposition.CREATE_IF_NEEDED,
        time_partitioning=TimePartitioning(type_=TimePartitioningType.DAY),
    )
    logger.debug("Beginning load")
    # try:
    bq_client.load_table_from_json(
        json_dict_list,
        destination=Table(f"{bq_project_id}.{bq_dataset_id}.{bq_table_id}"),
        job_config=job_config,
    )
    # TODO: Testing exceptions
    # except Exception as e:
    #     logger.error(e)
    #     raise e

    logger.debug("Load complete")
