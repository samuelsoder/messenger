from datetime import datetime
import sqlite3
import time
import uuid
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

    def connect(self):
        """
        Establishes connection to database, creates new if not exists.
        :return: None
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
            print(f'Connected to table {self.table_name}')
        except OperationalError as err:
            print(f'Failed to create table {self.table_name}, got error: {err}')

    def close(self):
        """
        Closes connection to table.
        :return: None
        """
        try:
            self.connection.close()
            self.connection = None
            print(f'Connection to {self.table_name} closed')
        except OperationalError as err:
            print(f'Failed to close connection to table {self.table_name}, got error: {err}')

    def insert_message(self, user_id, sender_id, message, date_sent=None):
        """
        Creates uuid and inserts message into database
        :param user_id: String, id of recipient
        :param sender_id: String, id of sender
        :param message: String, message
        :param date_sent: String, date message was sent (mainly for testing)
        :return: None
        """
        timestamp = convert_to_timestamp(date_sent) if date_sent else time.time()
        message_id = uuid.uuid4().hex

        command = Query \
            .into(self.table_name) \
            .insert(message_id, user_id, timestamp, sender_id, message)

        self.connection.execute(command.get_sql())
        self.connection.commit()

    def select_from_user(self, user_id, from_date=None, to_date=None):
        """
        Selects entries from the table for the given user between given dates
        :param user_id: String, id of user
        :param from_date: String, start date on format yyyy-mm-dd, epoch if not provided
        :param to_date: String, end date on format yyyy-mm-dd, current time if not provided
        :return: List of rows (id, user_id, timestamp, sender, message)
        """
        from_timestamp = convert_to_timestamp(from_date) if from_date else 0
        to_timestamp = convert_to_timestamp(to_date, end_of_day=True) if to_date else time.time()

        table = Table(self.table_name)
        command = Query \
            .from_(self.table_name) \
            .select('id', 'user_id', 'timestamp', 'sender', 'message') \
            .where((table.user_id == user_id) & (table.timestamp[from_timestamp:to_timestamp])) \
            .orderby('timestamp')

        cursor = self.connection.cursor()
        cursor.execute(command.get_sql())
        res = cursor.fetchall()
        cursor.close()
        return res

    def delete_messages(self, message_ids):
        """
        Deletes message with given id from table.
        :param message_ids: List of strings, ids of messages to delete
        :return: None
        """
        table = Table(self.table_name)
        command = Query \
            .from_(self.table_name) \
            .where(table.id.isin(message_ids)) \
            .delete()

        self.connection.execute(command.get_sql())
        self.connection.commit()

    def get_all(self):
        """
        Fetches all message in DB (mainly for testing)
        :return: List of rows (id, user_id, timestamp, sender, message)
        """
        command = Query \
            .from_(self.table_name) \
            .select('id', 'user_id', 'timestamp', 'sender', 'message')

        cursor = self.connection.cursor()
        cursor.execute(command.get_sql())
        res = cursor.fetchall()
        cursor.close()
        return res
