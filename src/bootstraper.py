from src.MessengerDB import MessengerDB


def init_db():
    """
    Initializes and connects the messenger db.
    :return: MessengerDB instance
    """
    db = MessengerDB('messenger')
    db.connect()

    return db
