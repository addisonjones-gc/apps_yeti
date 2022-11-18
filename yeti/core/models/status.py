from pydantic import BaseModel


class StreamStatus(BaseModel):
    PENDING = "Pending"
    COMPLETE = "Complete"
    ERROR = "Error"


class StreamStatusRequest(BaseModel):
    stream_id: str
    stream_status: StreamStatus
