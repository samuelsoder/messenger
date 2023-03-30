from typing import Union

from pydantic import BaseModel


class MessageRequest(BaseModel):
    """
    Model for POST requests for new messages
    """
    sender_id: str
    recipient_id: str
    message: str


class MessagePatch(BaseModel):
    """
    Model for patches, any none values are updated
    """
    sender_id: Union[str, None] = None
    recipient_id: Union[str, None] = None
    timestamp: Union[float, None] = None
    message: Union[str, None] = None


class Message:
    """
    Model for messages stored in the DB and returned through GET requests
    """
    id: str
    sender_id: str
    recipient_id: str
    timestamp: float
    message: str
