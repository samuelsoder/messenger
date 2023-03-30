from typing import Union

from pydantic import BaseModel


class MessageRequest(BaseModel):
    sender_id: str
    recipient_id: str
    message: str


class MessagePatch(BaseModel):
    sender_id: Union[str, None] = None
    recipient_id: Union[str, None] = None
    timestamp: Union[float, None] = None
    message: Union[str, None] = None


class Message:
    id: str
    sender_id: str
    recipient_id: str
    timestamp: float
    message: str
