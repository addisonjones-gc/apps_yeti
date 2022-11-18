from datetime import datetime
import json
import logging


def get_logger() -> logging.Logger:
    """Returns a standardized "yeti-logger" object

    :return: Returns standard logger object
    :rtype: logging.Logger
    """
    logging.basicConfig(
        level=logging.INFO,
        handlers=[logging.StreamHandler()],
    )

    return logging.getLogger("yeti-logger")


def add_ingest_time_to_json(
    json_data: str,
) -> str:
    ingest_time_string = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    serialized_json = json.loads(json_data)

    for record in serialized_json:
        record["_PARTITIONTIME"] = ingest_time_string
    return json.dumps(serialized_json)
