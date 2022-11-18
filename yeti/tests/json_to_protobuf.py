import pandas as pd
import logging
import json
from yeti.core.pkg.message_template import (
    create_append_request,
    StreamMessageTemplate,
)
from yeti.core.pkg.utils import (
    yield_append_requests_from_dataframe,
)

from google.cloud.bigquery_storage_v1beta2 import BigQueryWriteClient
from google.cloud.bigquery_storage_v1beta2.types import AppendRowsRequest

logging.basicConfig(
    # level=logging.INFO,
    level=logging.DEBUG,
    handlers=[
        # logging.FileHandler("debug.log"),
        logging.StreamHandler()
    ],
)

BQ_PROJECT_ID = "motorefi-analytics-dev"
BQ_DATASET_ID = "addison_sandbox"
BQ_TABLE_ID = "yeti_stream_test"

bq_write_client = BigQueryWriteClient()

write_stream = (
    BigQueryWriteClient.table_path(BQ_PROJECT_ID, BQ_DATASET_ID, BQ_TABLE_ID)
    + "/_default"
)


sample_json = [
    {
        "id": "1",
        "phone_number": "404-680-4624",
        # "_de_batch_id": "20221103201437",
        # "_de_ingest_timestamp": "2022-11-03 20:14:37.772885",
    },
    {
        "id": "2",
        "phone_number": "770-331-6978",
        # "_de_batch_id": "20221103201437",
        #     "_de_ingest_timestamp": "2022-11-03 20:14:37.772885",
    },
]
sample_json_str = json.dumps(sample_json)

json_df = pd.json_normalize(sample_json)
df_requests = yield_append_requests_from_dataframe(
    json_df, BQ_PROJECT_ID, BQ_DATASET_ID, BQ_TABLE_ID
)


json_field_name_list = []

for r in sample_json:
    for k in r:
        if k not in json_field_name_list:
            json_field_name_list.append(k)


json_protobuf_template = StreamMessageTemplate(json_field_name_list)
json_request = create_append_request(
    message_class=json_protobuf_template,
    input_json=sample_json_str,
    write_stream=write_stream,
)

requests = [json_request]
stream_response_iterator = bq_write_client.append_rows(requests=iter(requests))
for response in stream_response_iterator:
    print(response)
