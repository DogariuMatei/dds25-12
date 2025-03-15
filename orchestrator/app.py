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
    items: list  # List of items with quantities
    paid: bool = False


def send_post_request(url: str, json_data=None):
    try:
        response = requests.post(url, json=json_data)
    except requests.exceptions.RequestException:
        abort(400, REQ_ERROR_STR)
    else:
        return response


async def process_order(order: Order):
    # Step 1: Process stock
    stock_request = {
        "order_id": order.order_id,
        "items": [{"item_id": item_id, "quantity": quantity} for item_id, quantity in order.items]
    }
    stock_reply = await send_post_request(f"{GATEWAY_URL}/stock/process", stock_request)
    if stock_reply.status_code != 200:
        return f"Stock processing failed for order {order.order_id}"
    # Step 2: Process payment
    payment_reply = await send_post_request(f"{GATEWAY_URL}/payment/pay/{order.user_id}/{order.total_cost}")
    if payment_reply.status_code != 200:
        await send_post_request(f"{GATEWAY_URL}/stock/cancel", stock_request)
        return f"User payment failed for order {order.order_id}"
    # Successful
    order.paid = True
    try:
        await db.set(order.order_id, msgpack.encode(order))
    except redis.exceptions.RedisError:
        return f"Failed to save order {order.order_id} in DB"
        # Rollback everything xd
    await send_post_request(f"{GATEWAY_URL}/orders/order_status/{order.order_id}/{order.paid}")
    return f"Order {order.order_id} processed successfully"


@app.post('/handle')
def handle_order():
    request_data = request.get_json()
    user_id = request_data.get("user_id")
    order_id = request_data.get("order_id")
    total_cost = request_data.get("total_cost")
    items = request_data.get("items")
    order = Order(order_id, user_id, total_cost, items, False)

    stock_request = {
        "order_id": order.order_id,
        "items": [{"item_id": item["item_id"], "quantity": item["quantity"]} for item in items]
    }
    stock_reply = send_post_request(f"{GATEWAY_URL}/stock/process", stock_request)
    if stock_reply.status_code != 200:
        return Response("Checkout failed: Stock insufficient for Order {order.order_id}", status=400)
    # Step 2: Process payment
    payment_reply = send_post_request(f"{GATEWAY_URL}/payment/pay/{order.user_id}/{order.total_cost}")
    if payment_reply.status_code != 200:
        send_post_request(f"{GATEWAY_URL}/stock/cancel", stock_request)
        return Response("Checkout failed: Payment insufficient for Order {order.order_id}", status=400)
    # Successful
    order.paid = True
    return Response("Checkout successful", status=200)


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
