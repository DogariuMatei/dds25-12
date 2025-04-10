import logging
import os
import atexit
import random
import uuid
import threading
import time
from collections import defaultdict

import redis
import requests

from msgspec import msgpack, Struct
from flask import Flask, jsonify, abort, Response

DB_ERROR_STR = "DB error"
REQ_ERROR_STR = "Requests error"

GATEWAY_URL = os.environ['GATEWAY_URL']

app = Flask("order-service")

db: redis.Redis = redis.Redis(
    host=os.environ['REDIS_HOST'],
    port=int(os.environ['REDIS_PORT']),
    password=os.environ['REDIS_PASSWORD'],
    db=int(os.environ['REDIS_DB'])
)

def close_db_connection():
    db.close()

atexit.register(close_db_connection)

EVENT_REDIS_HOST = os.environ.get('EVENT_REDIS_HOST', 'localhost')
EVENT_REDIS_PORT = int(os.environ.get('EVENT_REDIS_PORT', 6379))
EVENT_REDIS_PASSWORD = os.environ.get('EVENT_REDIS_PASSWORD', '')
EVENT_REDIS_DB = int(os.environ.get('EVENT_REDIS_DB', 0))
event_db: redis.Redis = redis.Redis(
    host=EVENT_REDIS_HOST,
    port=EVENT_REDIS_PORT,
    password=EVENT_REDIS_PASSWORD,
    db=EVENT_REDIS_DB
)

class OrderValue(Struct):
    paid: bool
    items: list[tuple[str, int]]
    user_id: str
    total_cost: int

def get_order_from_db(order_id: str) -> OrderValue | None:
    try:
        entry: bytes = db.get(order_id)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    entry: OrderValue | None = msgpack.decode(entry, type=OrderValue) if entry else None
    if entry is None:
        abort(400, f"Order: {order_id} not found!")
    return entry

def publish_event(event: dict, stream: str = "events"):
    try:
        # Convert all values to strings (Redis expects bytes/strings)
        event_db.xadd(stream, {k: str(v) for k, v in event.items()})
        app.logger.debug(f"Published event: {event}")
    except redis.exceptions.RedisError as e:
        app.logger.error(f"Failed to publish event: {e}")

def process_event(event: dict):
    event_type = event.get(b'event_type', b'').decode()
    app.logger.info(f"[Order] Received event: {event}")
    if event_type == "payment_committed":
        app.logger.info("[Order] Payment committed event received.")

def listen_to_events(stream: str, last_id: str = "$"):
    app.logger.info(f"[Order] Started listening to stream '{stream}'")
    while True:
        try:
            events = event_db.xread({stream: last_id}, block=5000, count=1)
            if events:
                for s, messages in events:
                    for message_id, message in messages:
                        process_event(message)
                        last_id = message_id
        except redis.exceptions.RedisError as e:
            app.logger.error(f"Error reading stream {stream}: {e}")
            time.sleep(1)

@app.post('/create/<user_id>')
def create_order(user_id: str):
    key = str(uuid.uuid4())
    value = msgpack.encode(OrderValue(paid=False, items=[], user_id=user_id, total_cost=0))

    app.logger.debug(f"Creating order for user {user_id}")
    try:
        db.set(key, value)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    publish_event({"event_type": "order_created", "order_id": key, "user_id": user_id})
    return jsonify({'order_id': key})

@app.post('/batch_init/<n>/<n_items>/<n_users>/<item_price>')
def batch_init_users(n: int, n_items: int, n_users: int, item_price: int):

    n = int(n)
    n_items = int(n_items)
    n_users = int(n_users)
    item_price = int(item_price)

    def generate_entry() -> OrderValue:
        user_id = random.randint(0, n_users - 1)
        item1_id = random.randint(0, n_items - 1)
        item2_id = random.randint(0, n_items - 1)
        value = OrderValue(
            paid=False,
            items=[(f"{item1_id}", 1), (f"{item2_id}", 1)],
            user_id=f"{user_id}",
            total_cost=2 * item_price
        )
        return value

    kv_pairs: dict[str, bytes] = {f"{i}": msgpack.encode(generate_entry())
                                  for i in range(n)}
    try:
        db.mset(kv_pairs)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({"msg": "Batch init for orders successful"})

@app.get('/find/<order_id>')
def find_order(order_id: str):
    order_entry: OrderValue = get_order_from_db(order_id)
    return jsonify(
        {
            "order_id": order_id,
            "paid": order_entry.paid,
            "items": order_entry.items,
            "user_id": order_entry.user_id,
            "total_cost": order_entry.total_cost
        }
    )

def send_post_request(url: str):
    try:
        response = requests.post(url)
    except requests.exceptions.RequestException:
        abort(400, REQ_ERROR_STR)
    else:
        return response

def send_get_request(url: str):
    try:
        response = requests.get(url)
    except requests.exceptions.RequestException:
        abort(400, REQ_ERROR_STR)
    else:
        return response

@app.post('/addItem/<order_id>/<item_id>/<quantity>')
def add_item(order_id: str, item_id: str, quantity: int):
    order_entry: OrderValue = get_order_from_db(order_id)
    item_reply = send_get_request(f"{GATEWAY_URL}/stock/find/{item_id}")
    if item_reply.status_code != 200:
        abort(400, f"Item: {item_id} does not exist!")
    item_json: dict = item_reply.json()
    order_entry.items.append((item_id, int(quantity)))
    order_entry.total_cost += int(quantity) * item_json["price"]
    try:
        db.set(order_id, msgpack.encode(order_entry))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(f"Item: {item_id} added to: {order_id} price updated to: {order_entry.total_cost}",
                    status=200)

def rollback_stock(removed_items: list[tuple[str, int]]):
    for item_id, quantity in removed_items:
        send_post_request(f"{GATEWAY_URL}/stock/add/{item_id}/{quantity}")

@app.post('/checkout/<order_id>')
def checkout(order_id: str):
    app.logger.debug(f"Checking out order {order_id}")
    order_entry: OrderValue = get_order_from_db(order_id)

    transaction_id = str(uuid.uuid4())
    items_quantities: dict[str, int] = defaultdict(int)
    for (item_id, quantity) in order_entry.items:
        items_quantities[item_id] += quantity

    prepared_items = []
    for item_id, quantity in items_quantities.items():
        url_prepare_stock = f"{GATEWAY_URL}/stock/prepare_subtract/{transaction_id}/{item_id}/{quantity}"
        stock_resp = send_post_request(url_prepare_stock)
        if stock_resp.status_code != 200:
            send_post_request(f"{GATEWAY_URL}/stock/abort/{transaction_id}")
            abort(400, f"Failed to prepare stock for item {item_id}")
        prepared_items.append((item_id, quantity))
        app.logger.debug(f"SUCCESS PREP for item {item_id}")

    url_prepare_pay = f"{GATEWAY_URL}/payment/prepare_pay/{transaction_id}/{order_entry.user_id}/{order_entry.total_cost}"
    pay_resp = send_post_request(url_prepare_pay)
    if pay_resp.status_code != 200:
        send_post_request(f"{GATEWAY_URL}/stock/abort/{transaction_id}")
        abort(400, "Not enough credit to prepare payment")
    app.logger.debug(f"SUCCESS PREP PAY for order {order_id}")

    stock_commit_resp = send_post_request(f"{GATEWAY_URL}/stock/commit/{transaction_id}")
    if stock_commit_resp.status_code != 200:
        abort(500, "Failed to commit stock")

    payment_commit_resp = send_post_request(f"{GATEWAY_URL}/payment/commit/{transaction_id}")
    if payment_commit_resp.status_code != 200:
        abort(500, "Failed to commit payment")

    order_entry.paid = True
    try:
        db.set(order_id, msgpack.encode(order_entry))
    except redis.exceptions.RedisError:
        abort(400, DB_ERROR_STR)

    app.logger.debug("Checkout successful (2PC)")

    publish_event({"event_type": "order_completed", "order_id": order_id})
    return Response("Checkout successful (2PC)", status=200)

if __name__ == '__main__':
    listener_thread = threading.Thread(target=listen_to_events, args=("events",), daemon=True)
    listener_thread.start()
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
