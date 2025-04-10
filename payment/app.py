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
STOCK_EVENTS = "stock:events"
PAYMENT_EVENTS = "payment:events"

# Event types
STOCK_SUBTRACTED = "stock.subtracted" # we consume
PAYMENT_FAILED = "payment.failed" # we post

# Consumer groups
PAYMENT_STOCK_GROUP = "payment-stock-consumers"

app = Flask("payment-service")

db: redis.Redis = redis.Redis(host=os.environ['REDIS_HOST'],
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


class UserValue(Struct):
    credit: int

def add_to_response_stream(response_stream, order_id, status, reason=None):
    event_db.xadd(response_stream, {
        b'result': json.dumps({
            "status": status,
            "reason": reason,
            "order_id": order_id,
        }).encode()
    })

def publish_event(stream, event_type, data):
    """Publish an event to a Redis Stream"""
    event = {
        "type": event_type,
        "data": data,
        "transaction_id": data.get("transaction_id")
    }
    event_db.xadd(stream, {b'event': json.dumps(event).encode()})

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
    def wrapper(user_id, amount, logs_id, *args, **kwargs):
        lock_key = f"lock:user:{user_id}"
        lock_id = str(uuid.uuid4())
        lock_timeout = 5
        retry_count = 5
        lock_acquired = False

        # acquire lock with retries
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
            result = func(user_id, amount, logs_id, *args, **kwargs)
            return result
        finally:
            release_lock(lock_key, lock_id)

    return wrapper



def get_user_from_db(user_id: str, pipe) -> UserValue | None:
    try:
        # get serialized data
        entry: bytes = pipe.get(user_id)
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
    user_entry: UserValue = get_user_from_db(user_id, db)
    return jsonify(
        {
            "user_id": user_id,
            "credit": user_entry.credit
        }
    )

@app.post('/add_funds/<user_id>/<amount>')
def add_credit(user_id: str, amount: int):
    user_entry: UserValue = get_user_from_db(user_id, db)
    # update credit, serialize and update database
    user_entry.credit += int(amount)
    try:
        db.set(user_id, msgpack.encode(user_entry))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(f"User: {user_id} credit updated to: {user_entry.credit}", status=200)

@app.post('/pay/<user_id>/<amount>')
def remove_credit( user_id: str, amount: int):
    user_entry: UserValue = get_user_from_db(user_id, db)
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
def remove_credit_async(user_id, amount, logs_id):
    """Remove credit from a user with locking by decorator"""
    with db.pipeline() as pipe:
        try:
            pipe.watch(user_id)
            user_entry = get_user_from_db(user_id, pipe)
            amount = int(amount)

            if user_entry.credit < amount:
                return False

            user_entry.credit -= amount

            pipe.multi()
            pipe.set(user_id, msgpack.encode(user_entry))
            pipe.set(logs_id, "SUBTRACTED")
            pipe.execute()
            return True
        except Exception as e:
            app.logger.error(f"Error removing credit for user {user_id}: {e}")
            return False

def process_stock_events():
    """Process events from the stock stream"""
    consumer_name = f"payment-consumer-{uuid.uuid4()}"
    ensure_consumer_group(STOCK_EVENTS, PAYMENT_STOCK_GROUP)

    while True:
        try:
            messages = event_db.xreadgroup(
                PAYMENT_STOCK_GROUP,
                consumer_name,
                {STOCK_EVENTS: '>'},
                count=10,
                block=500
            )

            if not messages:
                continue

            for stream_name, stream_messages in messages:
                for message_id, message in stream_messages:
                    event = json.loads(message[b'event'].decode())
                    event_type = event.get('type')
                    event_data = event.get('data', {})

                    if event_type == STOCK_SUBTRACTED:
                        transaction_id = event_data.get('transaction_id')
                        order_id = event_data.get('order_id')
                        user_id = event_data.get('user_id')
                        total_cost = event_data.get('total_cost')
                        items = event_data.get('items', [])
                        response_stream= event_data.get('response_stream')

                        logs_id = f"logs{transaction_id}"
                        log_type = db.get(logs_id)
                        payment_result = remove_credit_async(user_id, int(total_cost), logs_id) if not log_type else True
                        if not payment_result:
                            add_to_response_stream(response_stream, order_id, "failed")
                            publish_event(PAYMENT_EVENTS, PAYMENT_FAILED, {
                                "order_id": order_id,
                                "user_id": user_id,
                                "transaction_id": transaction_id,
                                "items": items,
                                "total_cost": total_cost,
                                "response_stream": response_stream
                            })
                        else:
                            add_to_response_stream(response_stream, order_id, "success", "OK")

                    # Acknowledge the message
                    db.xack(stream_name, PAYMENT_STOCK_GROUP, message_id)

        except Exception as e:
            app.logger.error(f"Error in stock events consumer: {e}")
            time.sleep(0.1) #


def start_consumers():
    """Start background threads for event processing"""
    stock_consumer_thread = threading.Thread(target=process_stock_events, daemon=True)
    stock_consumer_thread.start()


def initialize_streams():
    """Initialize Redis Streams and consumer groups"""
    ensure_consumer_group(PAYMENT_EVENTS, "init-group")


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