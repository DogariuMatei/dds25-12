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

DB_ERROR_STR = "DB error"

# Stream keys
ORDER_EVENTS = "order:events"
STOCK_EVENTS = "stock:events"
PAYMENT_EVENTS = "payment:events"

# Event types
PAYMENT_RESERVED = "payment.reserved"  # We consume this
STOCK_ROLLBACK = "stock.rollback" # We consume this
STOCK_SUCCEEDED = "stock.succeeded"  # We post this
STOCK_FAILED = "stock.failed"  # We post this


# Consumer groups
STOCK_ORDER_GROUP = "stock-order-consumers"
STOCK_PAYMENT_GROUP = "stock-payment-consumers"

app = Flask("stock-service")

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


class StockValue(Struct):
    stock: int
    price: int
    reserved: int = 0


# Helper functions for Redis Streams
def publish_event(stream, event_type, data):
    """Publish an event to a Redis Stream"""
    event = {
        "type": event_type,
        "data": data,
        "transaction_id": data.get("transaction_id")
    }
    try:
        event_db.xadd(stream, {b'event': json.dumps(event).encode()})
        return event
    except Exception as e:
        app.logger.error(f"Failed to publish event: {e}")
        return None

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
    def wrapper(items, *args, **kwargs):
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
            result = func(items, *args, **kwargs)
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


@app.post('/item/create/<price>')
def create_item(price: int):
    key = str(uuid.uuid4())
    value = msgpack.encode(StockValue(stock=0, price=int(price), reserved=0))
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
    kv_pairs: dict[str, bytes] = {f"{i}": msgpack.encode(StockValue(stock=starting_stock, price=item_price, reserved=0))
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
            "reserved": item_entry.reserved,
            "available": item_entry.stock - item_entry.reserved
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
def add_or_subtract_stock_for_order(items, type):
    """Directly subtract stock for multiple items in a batch."""
    success = True
    failed_items = []

    for item in items:
        item_id = item.get('item_id')
        amount = item.get('amount')

        if not (item_id and amount):
            continue

        try:
            amount = int(amount)
            item_entry = get_item_from_db(item_id)

            if type == 'subtract':
                # Check stock availability
                if item_entry.stock < amount:
                    failed_items.append(item_id)
                    success = False
                    break

                # Directly subtract the stock
                item_entry.stock -= amount
            elif type == 'add':
                item_entry.stock += amount

            try:
                db.set(item_id, msgpack.encode(item_entry))
            except redis.exceptions.RedisError as e:
                app.logger.error(f"DB error when {type} stock: {e}")
                failed_items.append(item_id)
                success = False
                break

        except Exception as e:
            app.logger.error(f"Error processing stock {type} for item")
            failed_items.append(item_id)
            success = False
            break

    message = f"All stock {type} completed successfully" if success else f"Failed to {type} stock for items"
    return success, message


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
                count=10,
                block=5000
            )

            if not messages:
                continue

            for stream_name, stream_messages in messages:
                for message_id, message in stream_messages:
                    event = json.loads(message[b'event'].decode())

                    try:
                        event_type = event.get('type')
                        event_data = event.get('data', {})

                        if event_type == PAYMENT_RESERVED:
                            transaction_id = event_data.get('transaction_id')
                            order_id = event_data.get('order_id')
                            items = event_data.get('items', [])
                            total_cost = event_data.get('total_cost')
                            user_id = event_data.get('user_id')
                            response_stream = event_data.get('response_stream')

                            # Try to subtract stock directly
                            success, message = add_or_subtract_stock_for_order(items, 'subtract')

                            if not success:
                                # Stock subtraction failed, publish event for payment to cancel the reservation
                                publish_event(STOCK_EVENTS, STOCK_FAILED, {
                                    "order_id": order_id,
                                    "transaction_id": transaction_id,
                                    "user_id": user_id,
                                    "items": items,
                                    "total_cost": total_cost,
                                    "response_stream": response_stream,
                                    "reason": message
                                })
                            else:
                                # Stock subtraction succeeded, confirm the payment
                                publish_event(STOCK_EVENTS, STOCK_SUCCEEDED, {
                                    "order_id": order_id,
                                    "transaction_id": transaction_id,
                                    "user_id": user_id,
                                    "items": items,
                                    "total_cost": total_cost,
                                    "response_stream": response_stream
                                })

                        if event_type == STOCK_ROLLBACK:
                            order_id = event_data.get('order_id')
                            items = event_data.get('items', [])
                            response_stream = event_data.get('response_stream')

                            success,message = add_or_subtract_stock_for_order(items, 'add')

                            add_to_response_stream(response_stream, order_id, "failed",message)
                        # Acknowledge the message
                        event_db.xack(stream_name, STOCK_PAYMENT_GROUP, message_id)

                    except Exception as e:
                        app.logger.error(f"Error processing payment event: {e}")

        except Exception as e:
            app.logger.error(f"Error in payment events consumer: {e}")
            time.sleep(0.1)


def initialize_streams():
    """Initialize Redis Streams and consumer groups"""
    ensure_consumer_group(STOCK_EVENTS, "init-group")
    app.logger.info("Stock events stream initialized")

def start_consumers():
    """Start background threads for event processing"""
    payment_consumer_thread = threading.Thread(target=process_payment_events, daemon=True)
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