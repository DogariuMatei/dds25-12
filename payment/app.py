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

DB_ERROR_STR = "DB error"

# Stream keys
STOCK_EVENTS = "stock:events"
PAYMENT_EVENTS = "payment:events"

# Event types
STOCK_RESERVED = "stock.reserved" # we consume
PAYMENT_SUCCEEDED = "payment.succeeded" # we post
PAYMENT_FAILED = "payment.failed" # we post

# Consumer groups
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


def publish_event(stream, event_type, data):
    """Publish an event to a Redis Stream"""
    event = {
        "type": event_type,
        "data": data,
        "timestamp": int(time.time() * 1000),
        "transaction_id": data.get("transaction_id")
    }
    try:
        event_db.xadd(stream, {b'event': json.dumps(event).encode()})
        app.logger.debug(f"Published event {event_type} to {stream}")
        return event
    except Exception as e:
        app.logger.error(f"Failed to publish event: {e}")
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

    # Acquire lock for user
    lock_key = f"lock:user:{user_id}"
    lock_id = str(uuid.uuid4())

    # Try to acquire lock
    lock_acquired = db.set(lock_key, lock_id, nx=True, ex=10)
    if not lock_acquired:
        return abort(400, f"User {user_id} is currently being modified by another process")

    try:
        user_entry: UserValue = get_user_from_db(user_id)
        amount = int(amount)

        if user_entry.credit < amount:
            return abort(400, f"User: {user_id} does not have enough credit (has {user_entry.credit}, needs {amount})!")

        # Update credit
        user_entry.credit -= amount

        try:
            pipe = db.pipeline(transaction=True)
            pipe.set(user_id, msgpack.encode(user_entry))
            pipe.execute()
            return Response(f"User: {user_id} credit updated to: {user_entry.credit}", status=200)

        except redis.exceptions.RedisError:
            return abort(400, DB_ERROR_STR)
    finally:
        # Release lock
        script = """
        if redis.call('get', KEYS[1]) == ARGV[1] then
            return redis.call('del', KEYS[1])
        else
            return 0
        end
        """
        db.eval(script, 1, lock_key, lock_id)

def remove_credit_async( user_id: str, amount: int):
    app.logger.debug(f"Removing {amount} credit from user: {user_id}")

    # Acquire lock for user
    lock_key = f"lock:user:{user_id}"
    lock_id = str(uuid.uuid4())

    # Try to acquire lock
    lock_acquired = db.set(lock_key, lock_id, nx=True, ex=10)
    if not lock_acquired:
        return False

    try:
        user_entry: UserValue = get_user_from_db(user_id)
        amount = int(amount)

        if user_entry.credit < amount:
            return False

        # Update credit
        user_entry.credit -= amount

        try:
            pipe = db.pipeline(transaction=True)
            pipe.set(user_id, msgpack.encode(user_entry))
            pipe.execute()
            return True

        except redis.exceptions.RedisError:
            return False
    finally:
        # Release lock
        script = """
        if redis.call('get', KEYS[1]) == ARGV[1] then
            return redis.call('del', KEYS[1])
        else
            return 0
        end
        """
        db.eval(script, 1, lock_key, lock_id)


def add_to_response_stream(response_stream, status, order_id, reason=None):
    # Publish failed result to the order-service specific stream
    event_db.xadd(response_stream, {
        b'result': json.dumps({
            "status": status,
            "reason": reason,
            "order_id": order_id,
        }).encode()
    })


def process_stock_events():
    """Process events from the stock stream"""
    consumer_name = f"payment-consumer-{uuid.uuid4()}"
    ensure_consumer_group(STOCK_EVENTS, PAYMENT_STOCK_GROUP)
    app.logger.info(f"Starting stock events consumer {consumer_name}")

    while True:
        try:
            # Read new messages
            messages = event_db.xreadgroup(
                PAYMENT_STOCK_GROUP,
                consumer_name,
                {STOCK_EVENTS: '>'},
                count=10,
                block=2000
            )

            if not messages:
                continue

            for stream_name, stream_messages in messages:
                for message_id, message in stream_messages:
                    event = json.loads(message[b'event'].decode())

                    try:
                        event_type = event.get('type')
                        event_data = event.get('data', {})

                        app.logger.debug(f"Processing stock event: {event_type}")

                        if event_type == STOCK_RESERVED:
                            # Extract payment details from the event
                            transaction_id = event_data.get('transaction_id')
                            order_id = event_data.get('order_id')
                            user_id = event_data.get('user_id')
                            total_amount = event_data.get('total_cost')
                            items = event_data.get('items', [])
                            response_stream= event_data.get('response_stream')

                            app.logger.info(f"Processing payment for order {order_id}, amount {total_amount}")

                            try:
                                payment_result = remove_credit_async(user_id, total_amount)
                                if not payment_result:
                                    add_to_response_stream(response_stream, "failed", order_id, "PAYMENT_SUBTRACTION_FAILED")
                                    publish_event(PAYMENT_EVENTS, PAYMENT_FAILED, {
                                        "order_id": order_id,
                                        "user_id": user_id,
                                        "transaction_id": transaction_id,
                                        "items": items,
                                    })
                                else:
                                    # Publish payment success event
                                    add_to_response_stream(response_stream, "success", order_id, "PAYMENT_SUBTRACTED_SUCCESS")
                                    publish_event(PAYMENT_EVENTS, PAYMENT_SUCCEEDED, {
                                        "user_id": user_id,
                                        "transaction_id": transaction_id,
                                        "order_id": order_id,
                                        "items": items
                                    })
                                app.logger.info(f"Payment processing result: {payment_result}")

                            except Exception as e:
                                app.logger.error(f"Error processing payment: {e}")
                                add_to_response_stream(response_stream, "failed", order_id , "PAYMENT_PROCESSING_ERROR")
                                publish_event(PAYMENT_EVENTS, PAYMENT_FAILED, {
                                    "order_id": order_id,
                                    "transaction_id": transaction_id,
                                    "user_id": user_id,
                                    "amount": total_amount,
                                    "items": items,
                                })
                        # Acknowledge the message
                        db.xack(stream_name, PAYMENT_STOCK_GROUP, message_id)

                    except Exception as e:
                        app.logger.error(f"Error processing stock event {message_id}: {e}")
                        # Message will be reprocessed later if not acknowledged

        except Exception as e:
            app.logger.error(f"Error in stock events consumer: {e}")
            time.sleep(1)  # Prevent tight loop on persistent errors


def start_consumers():
    """Start background threads for event processing"""
    stock_consumer_thread = threading.Thread(target=process_stock_events, daemon=True)
    stock_consumer_thread.start()
    app.logger.info("Event consumers started")


def initialize_streams():
    """Initialize Redis Streams and consumer groups"""
    ensure_consumer_group(PAYMENT_EVENTS, "init-group")
    app.logger.info("Payment events stream initialized")



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