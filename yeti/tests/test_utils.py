from typing import List, Tuple
from uuid import uuid4


def generate_sample_json_schema_and_data(row_count: int) -> Tuple[dict, List[dict]]:
    """Helper function to generate a JSON dict list for unit testing

    :param row_count: Count of rows to generate
    :type row_count: int
    :return: _description_
    :rtype: List[dict]
    """
    sample_schema = {
        "id": "STRING",
        "uuid1": "STRING",
        "uuid2": "STRING",
        "uuid3": "STRING",
        "uuid4": "STRING",
        "uuid5": "STRING",
    }
    sample_json = [
        {
            "id": str(uuid4()),
            "uuid1": str(uuid4()),
            "uuid2": str(uuid4()),
            "uuid3": str(uuid4()),
            "uuid4": str(uuid4()),
            "uuid5": str(uuid4()),
        }
        for _ in range(0, row_count)
    ]

    return sample_schema, sample_json


print(generate_sample_json_schema_and_data(100))
