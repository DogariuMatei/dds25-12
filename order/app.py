import logging
import os
import atexit
import random
import uuid
import json
import time

import redis
import requests

from msgspec import msgpack, Struct
from flask import Flask, jsonify, abort, Response


DB_ERROR_STR = "DB error"
REQ_ERROR_STR = "Requests error"


# Stream keys
ORDER_EVENTS = "order:events"
PAYMENT_EVENTS = "payment:events"
STOCK_EVENTS = "stock:events"

# Event types
ORDER_CREATED = "order.created" # we post

# Consumer Groups
ORDER_STOCK_GROUP = "order-stock-consumer"
ORDER_PAYMENT_GROUP = "order-payment-consumer"


GATEWAY_URL = os.environ['GATEWAY_URL']

app = Flask("order-service")

db: redis.Redis = redis.Redis( host=os.environ['REDIS_HOST'],
                               port=int(os.environ['REDIS_PORT']),
                               password=os.environ['REDIS_PASSWORD'],
                               db=int(os.environ['REDIS_DB']))

event_db: redis.Redis = redis.Redis( host=os.environ.get('EVENT_REDIS_HOST', 'localhost'),
                        port=int(os.environ.get('REDIS_PORT', 6379)),
                        password=os.environ.get('REDIS_PASSWORD', ''),
                        db=int(os.environ.get('REDIS_DB', 0)))

def close_db_connection():
    db.close()


atexit.register(close_db_connection)


class OrderValue(Struct):
    paid: bool
    items: list[tuple[str, int]]
    user_id: str
    total_cost: int


def publish_event(stream, event_type, data):
    """Publish an event to a Redis Stream"""
    event = {
        "type": event_type,
        "data": data,
    }
    try:
        event_db.xadd(stream, {b'event': json.dumps(event).encode()})
        app.logger.info(f"Published event {event_type} to {stream}")
        return event
    except Exception as e:
        app.logger.info(f"Failed to publish event: {e}")
        return None


def ensure_consumer_group(stream, group):
    """Create a consumer group if it doesn't exist"""
    try:
        event_db.xgroup_create(stream, group, id='0-0', mkstream=True)
        app.logger.info(f"Created consumer group {group} for stream {stream}")
    except redis.exceptions.ResponseError as e:
        if 'BUSYGROUP' in str(e):  # Group already exists
            app.logger.debug(f"Consumer group {group} already exists for stream {stream}")
            pass
        else:
            app.logger.error(f"Error creating consumer group: {e}")
            raise

def get_order_from_db(order_id: str) -> OrderValue | None:
    try:
        # get serialized data
        entry: bytes = db.get(order_id)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    # deserialize data if it exists else return null
    entry: OrderValue | None = msgpack.decode(entry, type=OrderValue) if entry else None
    if entry is None:
        # if order does not exist in the database; abort
        abort(400, f"Order: {order_id} not found!")
    return entry


@app.post('/create/<user_id>')
def create_order(user_id: str):
    key = str(uuid.uuid4())
    value = msgpack.encode(OrderValue(paid=False, items=[], user_id=user_id, total_cost=0))
    try:
        db.set(key, value)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
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
        value = OrderValue(paid=False,
                           items=[(f"{item1_id}", 1), (f"{item2_id}", 1)],
                           user_id=f"{user_id}",
                           total_cost=2*item_price)
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
        # Request failed because item does not exist
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
    for item_id, amount in removed_items:
        send_post_request(f"{GATEWAY_URL}/stock/add/{item_id}/{amount}")

@app.post('/checkout/<order_id>')
def checkout(order_id: str):
    app.logger.info(f"Checking out {order_id}")

    order_entry: OrderValue = get_order_from_db(order_id)
    app.logger.info(f"Got order from db")
    # Check if order is already paid
    if order_entry.paid:
        return Response(f'Order {order_id} already paid", status=200')
    app.logger.info(f"Not already paid")
    formatted_items = []
    for item_id, amount in order_entry.items:
        formatted_items.append({
            "item_id": item_id,
            "amount": amount
        })

    # And a new response stream to check for success/failure
    transaction_id = str(uuid.uuid4())
    response_stream = f"checkout:response:{transaction_id}"
    app.logger.info(f"Created response stream for new transaction {response_stream}")

    # Create event data
    event_data = {
        "order_id": order_id,
        "transaction_id": transaction_id,
        "user_id": order_entry.user_id,
        "items": formatted_items,
        "total_cost": order_entry.total_cost,
        "response_stream": response_stream
    }

    # Publish the ORDER_CREATED event to order:events stream
    event = publish_event(ORDER_EVENTS, ORDER_CREATED, event_data)

    if not event:
        return abort(400, "Failed to publish order event")

    app.logger.debug(f"Published ORDER_CREATED event for order {order_id}, transaction {transaction_id}")

    # Wait for response
    timeout = 1
    start_time = time.time()

    # try:
    #     db.delete(response_stream)
    # except:
    #     pass

    # Poll for response with blocking call
    while time.time() - start_time < timeout:
        # Block for .1 seconds at a time
        messages = event_db.xread({response_stream: '0'}, count=1, block=100)
        # messages = event_db.xread({response_stream: '0'})

        if messages:
            stream_name, stream_messages = messages[0]
            message_id, message_data = stream_messages[0]

            result = json.loads(message_data[b'result'].decode())
            event_db.delete(response_stream)

            if result.get('status') == 'success':
                app.logger.info(f"Success order checkout id: {order_id}")
                return Response("Checkout successful", status=200)
            else:
                app.logger.info(f"Failed order checkout id: {order_id}")
                return abort(400, result.get('reason'))

    # Timeout occurred - return 400
    return Response("Checkout initiated but processing is still ongoing - Timeout", status=400)



def initialize_streams():
    """Initialize Redis Streams and consumer groups"""
    ensure_consumer_group(ORDER_EVENTS, "init-group")
    app.logger.info("Order events stream initialized")


def initialize_app():
    app.logger.info("Initializing order service")
    initialize_streams()


initialize_app()
if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
