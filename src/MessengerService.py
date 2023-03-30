from result import Result

from src.MessengerDB import MessengerDB
from src.models.models import MessageRequest, Message, MessagePatch


class MessengerService:
    """Class for handling any needed business logic, connects the endpoints with the DB."""

    def __init__(self, db):
        self.db: MessengerDB = db

    def get_all(self):
        return self.db.get_all()

    def get_messages(self, recipient_id: str, from_date: str, to_date: str) -> Result[list[Message], str]:
        return self.db.select_from_recipient(recipient_id, from_date, to_date)

    def add_message(self, message: MessageRequest) -> Result[str, str]:
        return self.db.insert_message(message.recipient_id, message.sender_id, message.message)

    def update_message(self, message_id: str, patch: MessagePatch):
        return self.db.patch_message(message_id, patch)

    def delete_messages(self, message_ids: list[str]) -> Result[str, str]:
        return self.db.delete_messages(message_ids)
