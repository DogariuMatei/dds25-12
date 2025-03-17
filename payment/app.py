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
STOCK_EVENTS = "stock:events"
PAYMENT_EVENTS = "payment:events"
ORDER_EVENTS = "order:events"

# Event types
ORDER_CREATED = "order.created"
PAYMENT_RESERVED = "payment.reserved"
STOCK_FAILED = "stock.failed"
STOCK_SUCCEEDED = "stock.succeeded"
STOCK_ROLLBACK = "stock.rollback"

# Consumer groups
PAYMENT_ORDER_GROUP = "payment-order-consumers"
PAYMENT_STOCK_GROUP = "payment-stock-consumers"

app = Flask("payment-service")

db: redis.Redis = redis.Redis(host=os.environ['REDIS_HOST'],
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


class UserValue(Struct):
    credit: int
    reserved: int = 0


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
        event_db.xgroup_create(stream, group, id='0-0', mkstream=True)
    except redis.exceptions.ResponseError as e:
        if 'BUSYGROUP' in str(e):  # Group already exists
            pass
        else:
            app.logger.error(f"Error creating consumer group: {e}")
            raise


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


def user_locking(func):
    """
    Decorator that implements two phase locking for user payment operations.
    """

    @wraps(func)
    def wrapper(user_id, amount, *args, **kwargs):
        lock_key = f"lock:user:{user_id}"
        lock_id = str(uuid.uuid4())
        lock_timeout = 5
        retry_count = 5
        lock_acquired = False

        # Try to acquire lock with retries
        for attempt in range(retry_count):
            lock_acquired = db.set(lock_key, lock_id, nx=True, ex=lock_timeout)
            if lock_acquired:
                break

            if attempt < retry_count - 1:
                backoff = min(0.1 * (2 ** attempt), 1.0)
                time.sleep(backoff)

        if not lock_acquired:
            app.logger.error(f"Could not acquire lock for user {user_id} after {retry_count} attempts")
            return False

        try:
            result = func(user_id, amount, *args, **kwargs)
            return result
        finally:
            release_lock(lock_key, lock_id)

    return wrapper



def get_user_from_db(user_id: str) -> UserValue | None:
    try:
        # get serialized data
        entry: bytes = db.get(user_id)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    # deserialize data if it exists else return null
    entry: UserValue | None = msgpack.decode(entry, type=UserValue) if entry else None
    if entry is None:
        # if user does not exist in the database; abort
        abort(400, f"User: {user_id} not found!")
    return entry


@app.post('/create_user')
def create_user():
    key = str(uuid.uuid4())
    value = msgpack.encode(UserValue(credit=0))
    try:
        db.set(key, value)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({'user_id': key})

@app.post('/batch_init/<n>/<starting_money>')
def batch_init_users(n: int, starting_money: int):
    n = int(n)
    starting_money = int(starting_money)
    kv_pairs: dict[str, bytes] = {f"{i}": msgpack.encode(UserValue(credit=starting_money))
                                  for i in range(n)}
    try:
        db.mset(kv_pairs)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({"msg": "Batch init for users successful"})

@app.get('/find_user/<user_id>')
def find_user(user_id: str):
    user_entry: UserValue = get_user_from_db(user_id)
    return jsonify(
        {
            "user_id": user_id,
            "credit": user_entry.credit
        }
    )

@app.post('/add_funds/<user_id>/<amount>')
def add_credit(user_id: str, amount: int):
    user_entry: UserValue = get_user_from_db(user_id)
    # update credit, serialize and update database
    user_entry.credit += int(amount)
    try:
        db.set(user_id, msgpack.encode(user_entry))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(f"User: {user_id} credit updated to: {user_entry.credit}", status=200)

@app.post('/pay/<user_id>/<amount>')
def remove_credit( user_id: str, amount: int):
    user_entry: UserValue = get_user_from_db(user_id)
    amount = int(amount)

    if user_entry.credit < amount:
        return abort(400, f"User: {user_id} does not have enough credit (has {user_entry.credit}, needs {amount})!")

    user_entry.credit -= amount

    try:
        pipe = db.pipeline(transaction=True)
        pipe.set(user_id, msgpack.encode(user_entry))
        pipe.execute()
        return Response(f"User: {user_id} credit updated to: {user_entry.credit}", status=200)

    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)


@user_locking
def reserve_credit(user_id, amount):
    """Reserve credit from a user with locking handled by decorator"""
    try:
        user_entry = get_user_from_db(user_id)
        amount = int(amount)

        # Check if user has enough available credit
        if user_entry.credit - user_entry.reserved < amount:
            return False, f"User: {user_id} does not have enough credit (has {user_entry.credit - user_entry.reserved} available, needs {amount})"

        # Mark the funds as reserved
        user_entry.reserved += amount

        try:
            pipe = db.pipeline(transaction=True)
            pipe.set(user_id, msgpack.encode(user_entry))
            pipe.execute()
            return True, "Credit reserved successfully"
        except redis.exceptions.RedisError as e:
            app.logger.error(f"DB error when reserving credit: {e}")
            return False, f"DB error when reserving credit: {e}"

    except Exception as e:
        app.logger.error(f"Error reserving credit for user {user_id}: {e}")
        return False, f"Error reserving credit: {e}"


# Function to confirm the payment when stock processing succeeds
@user_locking
def confirm_payment(user_id, amount):
    """Confirm reserved payment by converting reservation to actual payment"""
    try:
        user_entry = get_user_from_db(user_id)
        amount = int(amount)

        # Check that the amount is actually reserved
        if user_entry.reserved < amount:
            app.logger.error(
                f"Cannot confirm payment: user {user_id} has only {user_entry.reserved} reserved, trying to confirm {amount}")
            return False

        user_entry.credit -= amount
        user_entry.reserved -= amount

        try:
            pipe = db.pipeline(transaction=True)
            pipe.set(user_id, msgpack.encode(user_entry))
            pipe.execute()
            return True
        except redis.exceptions.RedisError as e:
            app.logger.error(f"DB error when confirming payment: {e}")
            return False
    except Exception as e:
        app.logger.error(f"Error confirming payment for user {user_id}: {e}")
        return False


# Function to cancel reservation if stock processing fails
@user_locking
def cancel_reservation(user_id, amount):
    """Cancel a credit reservation"""
    try:
        user_entry = get_user_from_db(user_id)

        if isinstance(amount, list):
            amount = sum(amount)
        amount = int(amount)

        # Verify there's enough reserved to cancel
        if user_entry.reserved < amount:
            user_entry.reserved = 0  # Reset to zero in case of inconsistency
        else:
            user_entry.reserved -= amount

        try:
            pipe = db.pipeline(transaction=True)
            pipe.set(user_id, msgpack.encode(user_entry))
            pipe.execute()
            return True
        except redis.exceptions.RedisError as e:
            app.logger.error(f"DB error when canceling reservation: {e}")
            return False
    except Exception as e:
        app.logger.error(f"Error canceling reservation for user {user_id}: {e}")
        return False


def process_order_events():
    """Process events from the order stream"""
    consumer_name_order = f"payment-order-consumer-{uuid.uuid4()}"
    ensure_consumer_group(ORDER_EVENTS, PAYMENT_ORDER_GROUP)

    while True:
        try:
            messages_order = event_db.xreadgroup(
                PAYMENT_ORDER_GROUP,
                consumer_name_order,
                {ORDER_EVENTS: '>'},
                count=1,
                block=5000
            )

            if not messages_order:
                continue

            for stream_name, stream_messages in messages_order:
                for message_id, message in stream_messages:
                    event = json.loads(message[b'event'].decode())
                    event_type = event.get('type')
                    event_data = event.get('data', {})

                    try:
                        if event_type == ORDER_CREATED:
                            transaction_id = event_data.get('transaction_id')
                            order_id = event_data.get('order_id')
                            user_id = event_data.get('user_id')
                            total_cost = event_data.get('total_cost')
                            items = event_data.get('items', [])
                            response_stream = event_data.get('response_stream')

                            payment_possible, reason = reserve_credit(user_id, int(total_cost))

                            if not payment_possible:
                                cancel_reservation(user_id, int(total_cost))
                                add_to_response_stream(response_stream, order_id, "failed", reason)
                            else:
                                publish_event(PAYMENT_EVENTS, PAYMENT_RESERVED, {
                                    "order_id": order_id,
                                    "user_id": user_id,
                                    "transaction_id": transaction_id,
                                    "items": items,
                                    "total_cost": total_cost,
                                    "response_stream": response_stream
                                })

                        event_db.xack(stream_name, PAYMENT_ORDER_GROUP, message_id)
                    except Exception as e:
                        app.logger.error(f"Error processing order event {event_type}: {e}")
        except Exception as e:
            app.logger.error(f"Error in order events consumer: {e}")
            time.sleep(0.1)

def process_stock_events():
    """Process events from the stock stream"""
    consumer_name_stock = f"payment-stock-consumer-{uuid.uuid4()}"
    ensure_consumer_group(STOCK_EVENTS, PAYMENT_STOCK_GROUP)

    while True:
        try:
            messages_stock = event_db.xreadgroup(
                PAYMENT_STOCK_GROUP,
                consumer_name_stock,
                {STOCK_EVENTS: '>'},
                count=1,
                block=5000
            )

            if not messages_stock:
                continue

            for stream_name, stream_messages in messages_stock:
                for message_id, message in stream_messages:
                    event = json.loads(message[b'event'].decode())
                    event_type = event.get('type')
                    event_data = event.get('data', {})
                    try:
                        if event_type == STOCK_SUCCEEDED:
                            transaction_id = event_data.get('transaction_id')
                            order_id = event_data.get('order_id')
                            user_id = event_data.get('user_id')
                            total_cost = event_data.get('total_cost')
                            items = event_data.get('items', [])
                            response_stream = event_data.get('response_stream')

                            # Complete the payment by finalizing the reserved amount
                            payment_confirmed = confirm_payment(user_id, int(total_cost))

                            if payment_confirmed:
                                add_to_response_stream(response_stream, order_id, "success","payment OK and stock OK")
                            else:
                                app.logger.error(f"Stock succeeded but payment confirmation failed for order {order_id}")
                                publish_event(PAYMENT_EVENTS, STOCK_ROLLBACK, {
                                    "order_id": order_id,
                                    "user_id": user_id,
                                    "transaction_id": transaction_id,
                                    "items": items,
                                    "total_cost": total_cost,
                                    "response_stream": response_stream,
                                })

                        if event_type == STOCK_FAILED:
                            order_id = event_data.get('order_id')
                            user_id = event_data.get('user_id')
                            total_cost = event_data.get('total_cost')
                            response_stream = event_data.get('response_stream')

                            # Cancel the payment reservation
                            reservation_canceled = cancel_reservation(user_id, int(total_cost))

                            if not reservation_canceled:
                                app.logger.error(f"Failed to cancel reservation for order {order_id}")
                            add_to_response_stream(response_stream, order_id, "failed", f"Order processing failed")

                        event_db.xack(stream_name, PAYMENT_STOCK_GROUP, message_id)
                    except Exception as e:
                        app.logger.error(f"Error processing order event {event_type}: {e}")
        except Exception as e:
            app.logger.error(f"Error in stock events consumer: {e}")
            time.sleep(0.1)


def start_consumers():
    """Start background threads for event processing"""
    order_consumer_thread = threading.Thread(target=process_order_events, daemon=True)
    stock_consumer_thread = threading.Thread(target=process_stock_events, daemon=True)
    order_consumer_thread.start()
    stock_consumer_thread.start()


def initialize_streams():
    ensure_consumer_group(PAYMENT_EVENTS, "init-group")
    ensure_consumer_group(STOCK_EVENTS, "init-group")


def initialize_app():
    app.logger.info("Initializing payment service")
    initialize_streams()
    start_consumers()

initialize_app()
if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)