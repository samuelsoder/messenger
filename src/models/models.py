from typing import Union

from pydantic import BaseModel


class MessageRequest(BaseModel):
    """
    Model for POST requests for new messages
    """
    sender_id: str
    recipient_id: str
    message: str
    date_sent: Union[str, None] = None


class MessagePatch(BaseModel):
    """
    Model for patches, any none values are updated
    """
    sender_id: Union[str, None] = None
    recipient_id: Union[str, None] = None
    date_sent: Union[str, None] = None
    message: Union[str, None] = None

