from datetime import datetime
import sqlite3
import time
import uuid
from src.models.models import Message, MessagePatch
from result import Ok, Err, Result
from sqlite3 import OperationalError
from pypika import Query, Table, Column


def convert_to_timestamp(date: str, end_of_day: bool = False):
    """
    Converts date in yyyy-mm-dd format to timestamp (seconds since epoch).
    :param date: String on format yyyy-mm-dd
    :param end_of_day: Boolean, if timestamp should be at end of day
    :return: Timestamp as float
    """
    # To get timestamp at end of day
    end_time = {
        'hour': 23,
        'minute': 59,
        'second': 59
    }
    return datetime.timestamp(datetime(*map(int, date.split('-')), **end_time if end_of_day else {}))


class MessengerDB:

    def __init__(self, table_name):
        self.table_name = table_name
        self.connection = None

    def connect(self) -> Result[str, str]:
        """
        Establishes connection to database, creates new if not exists.
        :return: Result with status
        """
        try:
            self.connection = sqlite3.connect(self.table_name)
            command = Query \
                .create_table(self.table_name) \
                .if_not_exists() \
                .columns(Column('id', 'STRING', nullable=False),
                         Column('recipient_id', 'STRING', nullable=False),
                         Column('timestamp', 'STRING', nullable=False),
                         Column('sender_id', 'STRING', nullable=False),
                         Column('message', 'STRING', nullable=False)
                         ) \
                .primary_key('id')
            self.connection.execute(command.get_sql())
            return Ok(f'Connected to table {self.table_name}')
        except OperationalError as err:
            return Err(f'Failed to create table {self.table_name}, got error: {err}')

    def close(self) -> Result[str, str]:
        """
        Closes connection to table.
        :return: Result with status
        """
        try:
            self.connection.close()
            self.connection = None
            return Ok('Connection to {self.table_name} closed')
        except OperationalError as err:
            return Err(f'Failed to close connection to table {self.table_name}, got error: {err}')

    def insert_message(self, recipient_id: str, sender_id: str, message: str, date_sent: str = None) \
            -> Result[str, str]:
        """
        Creates uuid and inserts message into database
        :param recipient_id: String, id of recipient
        :param sender_id: String, id of sender
        :param message: String, message
        :param date_sent: String, date message was sent (mainly for testing)
        :return: Result with status
        """
        timestamp = convert_to_timestamp(date_sent) if date_sent else time.time()
        message_id = uuid.uuid4().hex

        command = Query \
            .into(self.table_name) \
            .insert(message_id, recipient_id, timestamp, sender_id, message)

        try:
            self.connection.execute(command.get_sql())
            self.connection.commit()
            return Ok(f'Message successfully added to table {self.table_name}')
        except OperationalError as err:
            return Err(f'Failed to add message to table {self.table_name}, got error: {err}')

    def select_from_recipient(self, recipient_id: str, from_date: str = None, to_date: str = None) \
            -> Result[list[Message], str]:
        """
        Selects entries from the table for the given recipient between given dates
        :param recipient_id: String, id of recipient
        :param from_date: String, start date on format yyyy-mm-dd, epoch if not provided
        :param to_date: String, end date on format yyyy-mm-dd, current time if not provided
        :return: Result with list of rows (id, recipient_id, timestamp, sender_id, message) and string if failed
        """
        from_timestamp = convert_to_timestamp(from_date) if from_date else 0
        to_timestamp = convert_to_timestamp(to_date, end_of_day=True) if to_date else time.time()

        table = Table(self.table_name)
        command = Query \
            .from_(self.table_name) \
            .select('id', 'recipient_id', 'timestamp', 'sender_id', 'message') \
            .where((table.recipient_id == recipient_id) & (table.timestamp[from_timestamp:to_timestamp])) \
            .orderby('timestamp')

        try:
            cursor = self.connection.cursor()
            cursor.execute(command.get_sql())
            res = cursor.fetchall()
            cursor.close()
            return Ok(res)
        except OperationalError as err:
            return Err(f'Failed to select from table {self.table_name}, got error: {err}')

    def patch_message(self, message_id: str, message_patch: MessagePatch) -> Result[str, str]:
        """
        Patches message with given id.
        :param message_id: String
        :param message_patch: MessagePatch
        :return: Result with status
        """
        table = Table(self.table_name)
        attrs = ['recipient_id', 'timestamp', 'sender_id', 'message']

        not_none_attrs = filter(lambda attr: getattr(message_patch, attr) is not None, attrs)
        updates = map(lambda attr: Query.update(table)
                      .set(table[attr], getattr(message_patch, attr))
                      .where(table.id == message_id),
                      not_none_attrs
                      )

        try:
            for q in updates:
                self.connection.execute(q.get_sql())
            self.connection.commit()
            return Ok(f'Successfully updated message with id {message_id} in table {self.table_name}')
        except OperationalError as err:
            return Err(f'Failed to update item with id {message_id} from table {self.table_name}, got error: {err}')

    def delete_messages(self, message_ids: list[str]) -> Result[str, str]:
        """
        Deletes message with given id from table.
        :param message_ids: List of strings, ids of messages to delete
        :return: Result with status
        """
        table = Table(self.table_name)
        command = Query \
            .from_(self.table_name) \
            .where(table.id.isin(message_ids)) \
            .delete()

        try:
            self.connection.execute(command.get_sql())
            self.connection.commit()
            return Ok(f'Deleted messages with ids {message_ids} from table {self.table_name}')
        except OperationalError as err:
            return Err(f'Failed to delete items with ids {message_ids} from table {self.table_name}, got error: {err}')

    def get_all(self) -> Result[list[Message], str]:
        """
        Fetches all message in DB (mainly for testing)
        :return: Result with list of rows (id, recipient_id, timestamp, sender_id, message) and string if failed
        """
        command = Query \
            .from_(self.table_name) \
            .select('id', 'recipient_id', 'timestamp', 'sender_id', 'message')

        try:
            cursor = self.connection.cursor()
            cursor.execute(command.get_sql())
            res = cursor.fetchall()
            cursor.close()
            return Ok(res)
        except OperationalError as err:
            return Err(f'Failed to get all items from table {self.table_name}, got error: {err}')
