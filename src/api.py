from fastapi import FastAPI, Response, status

from src.MessengerService import MessengerService
from src.bootstraper import init_db
from src.models.models import MessageRequest, MessagePatch

app = FastAPI()
db = init_db()
service = MessengerService(db)


@app.get("/messenger/{recipient_id}", status_code=200)
async def get_messages(response: Response, recipient_id: str, from_date: str = None, to_date: str = None):
    get_result = db.select_from_recipient(recipient_id, from_date, to_date)
    if get_result.is_ok():
        return get_result.value
    else:
        response.status_code = status.HTTP_400_BAD_REQUEST
        return get_result.value


@app.post("/messenger/", status_code=201)
async def post_message(response: Response, message: MessageRequest):
    post_result = service.add_message(message)
    if post_result.is_ok():
        return post_result.value
    else:
        response.status_code = status.HTTP_400_BAD_REQUEST
        return post_result.value


@app.patch("/messenger/{message_id}", status_code=200)
async def patch_message(response: Response, message_id: str, patch: MessagePatch):
    patch_result = service.update_message(message_id, patch)
    if patch_result.is_ok():
        return patch_result.value
    else:
        response.status_code = status.HTTP_400_BAD_REQUEST
        return patch_result.value


@app.delete("/messenger/", status_code=200)
async def delete_messages(response: Response, *message_ids: str):
    delete_result = service.delete_messages(*message_ids)
    if delete_result.is_ok():
        return delete_result.value
    else:
        response.status_code = status.HTTP_400_BAD_REQUEST
        return delete_result.value
