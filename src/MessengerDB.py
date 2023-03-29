import time
import boto3
from botocore.exceptions import ClientError


class MessengerDB:

    def __init__(self, resource):
        self.resource = resource
        self.table_name = None
        self.table = None

    def create_table(self, table_name: str):
        try:
            table = self.resource.create_table(
                TableName=table_name,
                KeySchema=[
                    {'AttributeName': 'user_id', 'KeyType': 'HASH'},
                    {'AttributeName': 'timestamp', 'KeyType': 'RANGE'}
                ],
                AttributeDefinitions=[
                    {'AttributeName': 'user_id', 'AttributeType': 'S'},
                    {'AttributeName': 'timestamp', 'AttributeType': 'N'},
                ]
            )
            table.wait_until_exists()
        except ClientError as e:
            print(f'Failed to create table {table_name}, error: {e}')
        else:
            self.table_name = table_name
            self.table = table
            return self.table

    def delete_table(self):
        try:
            self.table.delete()
            self.table = None
        except ClientError as e:
            print(f'Failed to delete table {self.table_name}, error: {e}')

    def add_message(self, recipient_id, sender_id, message):
        now = time.time()

        try:
            self.table.put_item(
                Item={
                    'user_id': recipient_id,
                    'timestamp': now,
                    'data': {
                        'sender': sender_id,
                        'message': message,
                    },
                }
            )
        except ClientError as e:
            print(f'Failed to add item to table {self.table_name}, error: {e}')


