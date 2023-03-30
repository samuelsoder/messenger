from datetime import datetime
import sqlite3
import time
import uuid
from src.models.models import Message
from result import Ok, Err, Result
from sqlite3 import OperationalError
from pypika import Query, Table, Column


def convert_to_timestamp(date, end_of_day=False):
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
                         Column('user_id', 'STRING', nullable=False),
                         Column('timestamp', 'STRING', nullable=False),
                         Column('sender', 'STRING', nullable=False),
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

    def insert_message(self, user_id, sender_id, message, date_sent=None) -> Result[str, str]:
        """
        Creates uuid and inserts message into database
        :param user_id: String, id of recipient
        :param sender_id: String, id of sender
        :param message: String, message
        :param date_sent: String, date message was sent (mainly for testing)
        :return: Result with status
        """
        timestamp = convert_to_timestamp(date_sent) if date_sent else time.time()
        message_id = uuid.uuid4().hex

        command = Query \
            .into(self.table_name) \
            .insert(message_id, user_id, timestamp, sender_id, message)

        try:
            self.connection.execute(command.get_sql())
            self.connection.commit()
            return Ok(f'Message successfully added to table {self.table_name}')
        except OperationalError as err:
            return Err(f'Failed to add message to table {self.table_name}, got error: {err}')

    def select_from_user(self, user_id, from_date=None, to_date=None) -> Result[list[Message], str]:
        """
        Selects entries from the table for the given user between given dates
        :param user_id: String, id of user
        :param from_date: String, start date on format yyyy-mm-dd, epoch if not provided
        :param to_date: String, end date on format yyyy-mm-dd, current time if not provided
        :return: Result with list of rows (id, user_id, timestamp, sender, message) and string if failed
        """
        from_timestamp = convert_to_timestamp(from_date) if from_date else 0
        to_timestamp = convert_to_timestamp(to_date, end_of_day=True) if to_date else time.time()

        table = Table(self.table_name)
        command = Query \
            .from_(self.table_name) \
            .select('id', 'user_id', 'timestamp', 'sender', 'message') \
            .where((table.user_id == user_id) & (table.timestamp[from_timestamp:to_timestamp])) \
            .orderby('timestamp')

        try:
            cursor = self.connection.cursor()
            cursor.execute(command.get_sql())
            res = cursor.fetchall()
            cursor.close()
            return Ok(res)
        except OperationalError as err:
            return Err(f'Failed to select from table {self.table_name}, got error: {err}')

    def delete_messages(self, message_ids) -> Result[str, str]:
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
        :return: Result with list of rows (id, user_id, timestamp, sender, message) and string if failed
        """
        command = Query \
            .from_(self.table_name) \
            .select('id', 'user_id', 'timestamp', 'sender', 'message')

        try:
            cursor = self.connection.cursor()
            cursor.execute(command.get_sql())
            res = cursor.fetchall()
            cursor.close()
            return Ok(res)
        except OperationalError as err:
            return Err(f'Failed to get all items from table {self.table_name}, got error: {err}')
