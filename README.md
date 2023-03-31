# Messenger API

A REST-API for sending and receiving messages. Uses SQLite as DB.

## Local development

Make sure you have python 3.9 installed. 

Clone the repo and install required packages with

    pip install -r requirements.txt

Might have to install uvicorn with `pip install "uvicorn[standard]"` if not included in fastapi. Host the server locally with

    uvicorn src.api:app

View endpoints by accessing http://127.0.0.1:8000/docs

## Endpoints

* GET all messages: http://127.0.0.1:8000/messenger/
* GET messages by user id, specify time interval by query params from_date and to_date: http://127.0.0.1:8000/messenger/{user_id}?from_date={yyyy-mm-dd}&to_date={yyyy-mm-dd}
* POST with new message in body, check models in src/models: http://127.0.0.1:8000/messenger/
* PATCH with message patch in body, check models in src/models: http://127.0.0.1:8000/messenger/{message_id}
* DELETE with message ids to delete as query params: http://127.0.0.1:8000/messenger/?ids={id1}&ids={id2}