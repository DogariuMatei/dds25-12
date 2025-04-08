import logging
import os
import atexit
import uuid
import json
import time
import threading

import redis

from msgspec import msgpack, Struct
from flask import Flask, jsonify, abort, Response
from functools import wraps

from redis import WatchError

DB_ERROR_STR = "DB error"

# Stream keys
ORDER_EVENTS = "order:events"
STOCK_EVENTS = "stock:events"
PAYMENT_EVENTS = "payment:events"

# Event types
ORDER_CREATED = "order.created" # we consume
STOCK_RESERVED = "stock.reserved" # we post
STOCK_FAILED = "stock.failed" # we post
STOCK_RELEASED = "stock.released" # we post
PAYMENT_FAILED = "payment.failed" # we consume
PAYMENT_SUCCEEDED = "payment.succeeded" # we consume

# Consumer groups
STOCK_ORDER_GROUP = "stock-order-consumers"
STOCK_PAYMENT_GROUP = "stock-payment-consumers"

app = Flask("stock-service")

db: redis.Redis = redis.Redis( host=os.environ['REDIS_HOST'],
                               port=int(os.environ['REDIS_PORT']),
                               password=os.environ['REDIS_PASSWORD'],
                               db=int(os.environ['REDIS_DB']))

event_db: redis.Redis = redis.Redis( host=os.environ['EVENT_REDIS_HOST'],
                        port=int(os.environ['EVENT_REDIS_PORT']),
                        password=os.environ['EVENT_REDIS_PASSWORD'],
                        db=int(os.environ['EVENT_REDIS_DB']))

def close_db_connection():
    db.close()


atexit.register(close_db_connection)


class StockValue(Struct):
    stock: int
    price: int


# Helper functions for Redis Streams
def publish_event(stream, event_type, data):
    """Publish an event to a Redis Stream"""
    event = {
        "type": event_type,
        "data": data,
        "transaction_id": data.get("transaction_id")
    }
    event_db.xadd(stream, {b'event': json.dumps(event).encode()})

def add_to_response_stream(response_stream, order_id, status, reason=None):
    event_db.xadd(response_stream, {
        b'result': json.dumps({
            "status": status,
            "reason": reason,
            "order_id": order_id,
        }).encode()
    })

def ensure_consumer_group(stream, group):
    """Create a consumer group if it doesn't exist"""
    try:
        event_db.xgroup_create(stream, group, mkstream=True)

    except redis.exceptions.ResponseError as e:
        if 'BUSYGROUP' in str(e):  # Group already exists
            pass
        else:
            app.logger.error(f"Error creating consumer group: {e}")
            raise


def two_phase_locking(func):
    """Decorator that implements 2-Phase Locking for a batch of items."""

    @wraps(func)
    def wrapper(items, logs_id, *args, **kwargs):
        acquired_locks = {}
        lock_timeout = 5
        retry_count = 5

        # Acquire all locks
        for item in items:
            item_id = item.get('item_id')
            if not item_id:
                continue

            lock_key = f"lock:stock:{item_id}"
            lock_id = str(uuid.uuid4())

            # Try to acquire lock with retries
            for attempt in range(retry_count):
                lock_acquired = db.set(lock_key, lock_id, nx=True, ex=lock_timeout)
                if lock_acquired:
                    acquired_locks[lock_key] = lock_id
                    break

                if attempt < retry_count - 1:
                    time.sleep(0.1 * (2 ** attempt))

            # If we couldn't acquire this lock after retries, release all acquired locks and fail
            if lock_key not in acquired_locks:
                app.logger.error(f"Could not acquire lock for item {item_id} after {retry_count} attempts")
                # Release all locks we've acquired so far
                for key, id in acquired_locks.items():
                    release_lock(key, id)
                return False, "Could not acquire all required locks"

        try:
            # Phase 2: Execute the wrapped function to perform operations
            result = func(items, logs_id, *args, **kwargs)
            return result
        finally:
            for lock_key, lock_id in acquired_locks.items():
                release_lock(lock_key, lock_id)

    return wrapper


def release_lock(lock_key, lock_id):
    """Release a Redis lock using Lua script for atomicity"""
    script = """
    if redis.call('get', KEYS[1]) == ARGV[1] then
        return redis.call('del', KEYS[1])
    else
        return 0
    end
    """
    return db.eval(script, 1, lock_key, lock_id)



def get_item_from_db(item_id: str) -> StockValue | None:
    try:
        entry: bytes = db.get(item_id)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    # deserialize data if it exists else return null
    entry: StockValue | None = msgpack.decode(entry, type=StockValue) if entry else None
    if entry is None:
        # if item does not exist in the database; abort
        abort(400, f"Item: {item_id} not found!")
    return entry


def get_items_from_db(item_ids, pipe):
    """
    Get items from the database in a single operation
    """
    if not item_ids:
        return {}

    items = {}

    for item_id in item_ids:
        entry = pipe.get(item_id)
        items[item_id] = msgpack.decode(entry, type=StockValue)

    return items


@app.post('/item/create/<price>')
def create_item(price: int):
    key = str(uuid.uuid4())
    value = msgpack.encode(StockValue(stock=0, price=int(price)))
    try:
        db.set(key, value)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({'item_id': key})

@app.post('/batch_init/<n>/<starting_stock>/<item_price>')
def batch_init_users(n: int, starting_stock: int, item_price: int):
    n = int(n)
    starting_stock = int(starting_stock)
    item_price = int(item_price)
    kv_pairs: dict[str, bytes] = {f"{i}": msgpack.encode(StockValue(stock=starting_stock, price=item_price))
                                  for i in range(n)}
    try:
        db.mset(kv_pairs)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({"msg": "Batch init for stock successful"})

@app.get('/find/<item_id>')
def find_item(item_id: str):
    item_entry: StockValue = get_item_from_db(item_id)
    return jsonify(
        {
            "stock": item_entry.stock,
            "price": item_entry.price,
        }
    )

@app.post('/add/<item_id>/<amount>')
def add_stock(item_id: str, amount: int):
    item_entry: StockValue = get_item_from_db(item_id)
    # update stock, serialize and update database
    item_entry.stock += int(amount)
    try:
        db.set(item_id, msgpack.encode(item_entry))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(f"Item: {item_id} stock updated to: {item_entry.stock}", status=200)

@app.post('/subtract/<item_id>/<amount>')
def remove_stock(item_id: str, amount: int):
    item_entry: StockValue = get_item_from_db(item_id)
    # update stock, serialize and update database
    item_entry.stock -= int(amount)
    app.logger.debug(f"Item: {item_id} stock updated to: {item_entry.stock}")
    if item_entry.stock < 0:
        abort(400, f"Item: {item_id} stock cannot get reduced below zero!")
    try:
        db.set(item_id, msgpack.encode(item_entry))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(f"Item: {item_id} stock updated to: {item_entry.stock}", status=200)


@two_phase_locking
def make_reservations(items, logs_id):
    """
    Make reservations for multiple items using Redis Pipeline.
    """
    success = True
    failed_items = []

    item_ids = [item.get('item_id') for item in items if item.get('item_id') and item.get('amount')]

    with db.pipeline() as pipe:
        try:
            pipe.watch(*item_ids)
            item_entries_dict = get_items_from_db(item_ids, pipe)

            item_updates = {}
            for item in items:
                item_id = item.get('item_id')
                amount = item.get('amount')

                if not (item_id and amount):
                    continue

                amount = int(amount)

                if item_id not in item_entries_dict:
                    failed_items.append(item_id)
                    success = False
                    break

                item_entry = item_entries_dict[item_id]

                if item_entry.stock < amount:
                    failed_items.append(item_id)
                    success = False
                    break

                item_updates[item_id] = {
                    'entry': item_entry,
                    'amount': amount
                }

            if success:
                pipe.multi()
                for item_id, data in item_updates.items():
                    item_entry = data['entry']
                    amount = data['amount']

                    item_entry.stock -= amount

                    pipe.set(item_id, msgpack.encode(item_entry))

                pipe.set(logs_id, "SUBTRACTED")
                pipe.execute()

            message = "All reservations completed successfully" if success else f"Failed to reserve items: {', '.join(failed_items)}"
            return success, message
        except redis.exceptions.RedisError as e:
            app.logger.error(f"Redis pipeline error when making reservations: {e}")
            success = False
            message = f"Failed to reserve items due to database error"
            return success, message


@two_phase_locking
def release_reservations(items, logs_id):
    """
    Release reservations for multiple items using Redis Pipeline.
    """
    item_ids = [item.get('item_id') for item in items if item.get('item_id') and item.get('amount')]

    with db.pipeline() as pipe:
        pipe.watch(*item_ids)
        item_entries_dict = get_items_from_db(item_ids, pipe)

        item_updates = {}
        for item in items:
            item_id = item.get('item_id')
            amount = item.get('amount')

            if not (item_id and amount):
                continue

            amount = int(amount)

            if item_id not in item_entries_dict:
                continue

            item_entry = item_entries_dict[item_id]

            item_updates[item_id] = {
                'entry': item_entry,
                'amount': amount
            }

        pipe.multi()
        for item_id, data in item_updates.items():
            item_entry = data['entry']
            amount = data['amount']

            item_entry.stock += amount

            pipe.set(item_id, msgpack.encode(item_entry))

        pipe.set(logs_id, "ADDED")
        pipe.execute()


def process_order_events():
    """Process events from the order stream"""
    consumer_name = f"stock-consumer-{uuid.uuid4()}"
    ensure_consumer_group(ORDER_EVENTS, STOCK_ORDER_GROUP)

    while True:
        try:
            message = event_db.xreadgroup(
                STOCK_ORDER_GROUP,
                consumer_name,
                {ORDER_EVENTS: '>'},
                count=1,
                block=500
            )

            if not message:
                continue

            for stream_name, stream_messages in message:
                for message_id, message in stream_messages:
                    event = json.loads(message[b'event'].decode())

                    event_type = event.get('type')
                    event_data = event.get('data', {})

                    if event_type == ORDER_CREATED:
                        transaction_id = event_data.get('transaction_id')
                        order_id = event_data.get('order_id')
                        items = event_data.get('items', [])
                        total_cost = event_data.get('total_cost')
                        user_id = event_data.get('user_id')
                        response_stream = event_data.get('response_stream')

                        logs_id = f"logs{transaction_id}"
                        log_type = db.get(logs_id)
                        # Try to reserve
                        success, message = make_reservations(items, logs_id) if not log_type else (True, None)

                        if not success:
                            add_to_response_stream(response_stream, order_id, "failed", message)

                        else:
                            publish_event(STOCK_EVENTS, STOCK_RESERVED, {
                                "order_id": order_id,
                                "transaction_id": transaction_id,
                                "total_cost": total_cost,
                                "user_id": user_id,
                                "items": items,
                                "response_stream": response_stream
                            })

                    # Acknowledge the message
                    event_db.xack(stream_name, STOCK_ORDER_GROUP, message_id)

        except Exception as e:
            app.logger.error(f"Error in order events consumer: {e}")
            time.sleep(1) #


def process_payment_events():
    """Process events from the payment stream"""
    consumer_name = f"stock-payment-consumer-{uuid.uuid4()}"
    ensure_consumer_group(PAYMENT_EVENTS, STOCK_PAYMENT_GROUP)

    while True:
        try:
            messages = event_db.xreadgroup(
                STOCK_PAYMENT_GROUP,
                consumer_name,
                {PAYMENT_EVENTS: '>'},
                count=1,
                block=500
            )

            if not messages:
                continue

            for stream_name, stream_messages in messages:
                for message_id, message in stream_messages:
                    event = json.loads(message[b'event'].decode())

                    event_type = event.get('type')
                    event_data = event.get('data', {})

                    if event_type == PAYMENT_FAILED:
                        order_id = event_data.get('order_id')
                        items = event_data.get('items', [])
                        response_stream = event_data.get('response_stream')


                        logs_id = f"logs{event_data.get('transaction_id')}"
                        log_type = db.get(logs_id)
                        if log_type == b"SUBTRACTED":
                            release_reservations(items, logs_id)

                    db.xack(stream_name, STOCK_PAYMENT_GROUP, message_id)

        except Exception as e:
            app.logger.error(f"Error in payment events consumer: {e}")
            time.sleep(1)


def initialize_streams():
    """Initialize Redis Streams and consumer groups"""
    ensure_consumer_group(STOCK_EVENTS, "init-group")
    app.logger.info("Stock events stream initialized")

def start_consumers():
    """Start background threads for event processing"""
    order_consumer_thread = threading.Thread(target=process_order_events, daemon=True)
    payment_consumer_thread = threading.Thread(target=process_payment_events, daemon=True)

    order_consumer_thread.start()
    payment_consumer_thread.start()


def initialize_app():
    app.logger.info("Initializing stock service event processing")
    initialize_streams()
    start_consumers()

initialize_app()
if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)