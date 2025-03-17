import logging
import os
import atexit
import uuid
import redis
import asyncio

import requests
from msgspec import msgpack, Struct
from flask import Flask, jsonify, Response, request, abort
import aiohttp
from redis import Redis
from rq import Queue
from rq.job import Job

import redis

from msgspec import msgpack, Struct
from flask import Flask, jsonify, abort, Response, request

DB_ERROR_STR = "DB error"
REQ_ERROR_STR = "Requests error"

GATEWAY_URL = os.environ['GATEWAY_URL']

app = Flask("orchestrator-service")

db: redis.Redis = redis.Redis(host=os.environ['REDIS_HOST'],
                              port=int(os.environ['REDIS_PORT']),
                              password=os.environ['REDIS_PASSWORD'],
                              db=int(os.environ['REDIS_DB']))
queue = Queue(connection=db)


def close_db_connection():
    db.close()


atexit.register(close_db_connection)


class Order(Struct):
    order_id: str
    user_id: str
    total_cost: int
    items: list
    paid: bool = False
    status: str
    error: str = None


def get_item_from_db(order_id: str) -> Order | None:
    # get serialized data
    try:
        entry: bytes = db.get(order_id)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    # deserialize data if it exists else return null
    entry: Order | None = msgpack.decode(entry, type=Order) if entry else None
    if entry is None:
        # if item does not exist in the database; abort
        abort(400, f"Order: {order_id} not found!")
    return entry


def send_post_request(url: str, json_data=None):
    try:
        response = requests.post(url, json=json_data, timeout=5)
    except requests.exceptions.RequestException:
        abort(400, REQ_ERROR_STR)
    else:
        return response


def process_order(order_id: str):
    order: Order = get_item_from_db(order_id)

    stock_request = {
        "order_id": order.order_id,
        "items": [{"item_id": item["item_id"], "quantity": item["quantity"]} for item in order.items]
    }
    stock_reply = send_post_request(f"{GATEWAY_URL}/stock/process", stock_request)
    if stock_reply.status_code != 200:
        order.status = "CANCELLED"
        order.error = stock_reply.text

    # Step 2: Process payment
    payment_reply = send_post_request(f"{GATEWAY_URL}/payment/pay/{order.user_id}/{order.total_cost}")
    if payment_reply.status_code != 200:
        send_post_request(f"{GATEWAY_URL}/stock/cancel", stock_request)
        order.status = "CANCELLED"
        order.error = payment_reply.text

    # Successful
    if order.status != "CANCELLED":
        order.paid = True
        order.status = "COMPLETED"


@app.post('/receive')
def receive_order():
    request_data = request.get_json()
    user_id = request_data.get("user_id")
    order_id = request_data.get("order_id")
    total_cost = request_data.get("total_cost")
    items = request_data.get("items")

    value = msgpack.encode(Order(order_id, user_id, total_cost, items, False, "QUEUED"))
    try:
        db.set(order_id, value)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)

    queue.enqueue(process_order, order_id)
    return Response("Order successfully queued", status=200)


@app.get('/status/<order_id>')
def order_status(order_id: str):
    order: Order = get_item_from_db(order_id)
    return jsonify({'order_status': order.status, 'order_error': order.error})



if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
