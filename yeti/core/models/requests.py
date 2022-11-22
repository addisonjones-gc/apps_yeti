from typing import List, Union
from pydantic import BaseModel


class YetiRequest(BaseModel):
    """YetiRequest represents the structure for an inbound stream request to Yeti

    :param bq_project_id str: Name of the target BigQuery project
    :param bq_dataset_id str: Name of the target BigQuery dataset
    :param bq_table_id str: Name of the target BigQuery table
    :param json_schema dict: Dictionary containing a key: value pair of
           fields from the request and their datatypes. Valid dataypes
           include this subset of BigQuery known datatypes:
           - STRING
           - TIMESTAMP
           - DATE
           - DATETIME
           - FLOAT64
           - INT64

           Example:
           {
             "id": "STRING",
             "created_time": "TIMESTAMP",
             "loan_id": "INT64"
           }

    :param json_data List: A JSON serializable list containing key: value pairs
           of data to ingest
           Example:
           [
            {"id": "abc123", "created_time": "2022-11-01 16:00:00", "loan_id": 57},
            {"id": "xyz789", "created_time": "2022-11-01 16:00:01", "loan_id": 80}
           ]
           OR:
           {"id": "xyz789", "created_time": "2022-11-01 16:00:01", "loan_id": 80}
    """

    bq_project_id: str
    bq_dataset_id: str
    bq_table_id: str
    json_schema: dict
    json_data: Union[List, dict]
