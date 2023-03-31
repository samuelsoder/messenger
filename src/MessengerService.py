from typing import Union

from result import Result

from src.MessengerDB import MessengerDB
from src.models.models import MessageRequest, MessagePatch


def to_message(message_id: str, recipient_id: str, timestamp: float, sender_id: str, message: str) -> dict:
    return {
        'id': message_id,
        'recipient_id': recipient_id,
        'sender_id': sender_id,
        'timestamp': timestamp,
        'message': message
    }


def map_to_messages_dict(db_rows: list[Union[str, float]]) -> list[dict]:
    """
    Maps the rows returned from the DB to dicts.
    :param db_rows: List of lists in format ('id', 'recipient_id', 'timestamp', 'sender_id', 'message')
    :return: List of dicts
    """
    return list(map(lambda row: to_message(*row), db_rows))


class MessengerService:
    """Class for handling any needed business logic, connects the endpoints with the DB."""

    def __init__(self, db):
        self.db: MessengerDB = db

    def get_all(self):
        return self.db.get_all().map(
            lambda rows: map_to_messages_dict(rows)
        )

    def get_messages(self, recipient_id: str, from_date: str, to_date: str) -> Result[list[dict], str]:
        return self.db.select_from_recipient(recipient_id, from_date, to_date).map(
            lambda rows: map_to_messages_dict(rows)
        )

    def add_message(self, message: MessageRequest) -> Result[str, str]:
        return self.db.insert_message(message.recipient_id, message.sender_id, message.message, message.date_sent)

    def update_message(self, message_id: str, patch: MessagePatch):
        return self.db.patch_message(message_id, patch)

    def delete_messages(self, message_ids: list[str]) -> Result[str, str]:
        return self.db.delete_messages(message_ids)
