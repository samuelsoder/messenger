from src.MessengerDB import MessengerDB


def init_db():
    """
    Initializes and connects the messenger db.
    :return: MessengerDB instance
    """
    db = MessengerDB('messenger')
    connecting = db.connect()
    if connecting.is_ok():
        print(connecting.value)
        return db
    else:
        print(connecting.value)
        raise Exception
