import json
import requests

# sample_json = [
#     {
#         "id": "1",
#         "phone_number": "404-680-4624",
#         # "_de_batch_id": "20221103201437",
#         # "_de_ingest_timestamp": "2022-11-03 20:14:37.772885",
#     },
#     {
#         "id": "2",
#         "phone_number": "770-331-6978",
#         # "_de_batch_id": "20221103201437",
#         #     "_de_ingest_timestamp": "2022-11-03 20:14:37.772885",
#     },
# ]

with open("../../scripts/10k_sample.json", "r", encoding="utf-8") as f:
    input_json = json.loads(f.read())

# print(input_json[0])
print(len(input_json))
# exit(0)

body_json = {
    "bq_project_id": "motorefi-analytics-dev",
    "bq_dataset_id": "addison_sandbox",
    "bq_table_id": "yeti_stream_test",
    "input_json": input_json,
}

resp = requests.post("http://127.0.0.1:8000/stream_json", json=body_json)
print(resp)
