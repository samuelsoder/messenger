from fastapi import FastAPI, Response, status

from src.MessengerService import MessengerService
from src.bootstraper import init_db
from src.models.models import MessageRequest, MessagePatch

app = FastAPI()
db = init_db()
service = MessengerService(db)


@app.get("/messenger/", status_code=200)
async def get_all(response: Response):
    """
    Endpoint for fetching all messages, mainly for testing.
    :param response: Response, object for modifying response
    :return: List of Messages
    """
    get_result = service.get_all()
    if get_result.is_ok():
        return get_result.value
    else:
        response.status_code = status.HTTP_400_BAD_REQUEST
        return get_result.value


@app.get("/messenger/{recipient_id}", status_code=200)
async def get_messages(response: Response, recipient_id: str, from_date: str = None, to_date: str = None):
    """
    Endpoint for getting messages for a given recipient
    :param response: Response, object for modifying response
    :param recipient_id:
    :param from_date: String, from which date to fetch messages
    :param to_date: String, to which date to fetch messages
    :return: List of Messages
    """
    get_result = service.get_messages(recipient_id, from_date, to_date)
    if get_result.is_ok():
        return get_result.value
    else:
        response.status_code = status.HTTP_400_BAD_REQUEST
        return get_result.value


@app.post("/messenger/", status_code=201)
async def post_message(response: Response, message: MessageRequest):
    """
    Endpoint for posting new message
    :param response: Response, object for modifying response
    :param message: MessageRequest to post
    :return: String with result
    """
    post_result = service.add_message(message)
    if post_result.is_ok():
        return post_result.value
    else:
        response.status_code = status.HTTP_400_BAD_REQUEST
        return post_result.value


@app.patch("/messenger/{message_id}", status_code=200)
async def patch_message(response: Response, message_id: str, patch: MessagePatch):
    """
    Endpoint for patching message with given message id
    :param response: Response, object for modifying response
    :param message_id: String, id of message to patch
    :param patch: MessagePatch, object with attributes to patch
    :return: String with result
    """
    patch_result = service.update_message(message_id, patch)
    if patch_result.is_ok():
        return patch_result.value
    else:
        response.status_code = status.HTTP_400_BAD_REQUEST
        return patch_result.value


@app.delete("/messenger/", status_code=200)
async def delete_messages(response: Response, *message_ids: str):
    """
    Endpoint for deleting messages provided as queryparams
    :param response: Response, object for modifying response
    :param message_ids: List of strings, ids of messages to delete
    :return:
    """
    delete_result = service.delete_messages(*message_ids)
    if delete_result.is_ok():
        return delete_result.value
    else:
        response.status_code = status.HTTP_400_BAD_REQUEST
        return delete_result.value
