from datetime import datetime
import logging
from typing import List, Union


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
    json_dict_list: List,
) -> List[dict]:
    # ingest_time_string = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    ingest_time_string = datetime.utcnow().strftime("%Y-%m-%d 00:00:00 UTC")
    print(ingest_time_string)

    for record in json_dict_list:
        record["_PARTITIONTIME"] = ingest_time_string
    return json_dict_list


def ensure_dict_list(input_json: Union[List, dict]) -> List[dict]:
    """Function to ensure types passed between methods is of type
       List[dict]

    :param input_json: Input of JSON serializable List or dict
    :type input_json: Union[List, dict]
    :return: Returns appropriate type for use across Yeti functions
    :rtype: List[dict]
    """
    if isinstance(input_json, dict):
        return list(input_json)
    else:
        return input_json
