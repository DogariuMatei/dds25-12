import logging
import os
import atexit
import uuid
import threading
import time

import redis

from msgspec import msgpack, Struct
from flask import Flask, jsonify, abort, Response

DB_ERROR_STR = "DB error"

app = Flask("payment-service")

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

class UserValue(Struct):
    credit: int

def get_user_from_db(user_id: str) -> UserValue | None:
    try:
        entry: bytes = db.get(user_id)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    entry: UserValue | None = msgpack.decode(entry, type=UserValue) if entry else None
    if entry is None:
        abort(400, f"User: {user_id} not found!")
    return entry

def publish_event(event: dict, stream: str = "events"):
    try:
        event_db.xadd(stream, {k: str(v) for k, v in event.items()})
        app.logger.debug(f"Published event: {event}")
    except redis.exceptions.RedisError as e:
        app.logger.error(f"Failed to publish event: {e}")

def process_event(event: dict):
    event_type = event.get(b'event_type', b'').decode()
    app.logger.info(f"[Payment] Received event: {event}")
    if event_type == "order_completed":
        app.logger.info("[Payment] Order completed event received.")

def listen_to_events(stream: str, last_id: str = "$"):
    app.logger.info(f"[Payment] Started listening to stream '{stream}'")
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

@app.post('/create_user')
def create_user():
    key = str(uuid.uuid4())
    value = msgpack.encode(UserValue(credit=0))
    try:
        db.set(key, value)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    publish_event({"event_type": "user_created", "user_id": key})
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
    user_entry.credit += int(amount)
    try:
        db.set(user_id, msgpack.encode(user_entry))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    publish_event({"event_type": "funds_added", "user_id": user_id, "amount": amount})
    return Response(f"User: {user_id} credit updated to: {user_entry.credit}", status=200)

@app.post("/prepare_pay/<txn_id>/<user_id>/<amount>")
def prepare_pay(txn_id: str, user_id: str, amount: int):
    amt = int(amount)
    with db.pipeline() as pipe:
        try:
            pipe.watch(user_id)
            current_data = db.get(user_id)
            if current_data is None:
                pipe.unwatch()
                abort(400, f"User: {user_id} not found!")
            user_obj: UserValue = msgpack.decode(current_data, type=UserValue)
            if user_obj.credit < amt:
                pipe.unwatch()
                abort(400, "Not enough credit")
            user_obj.credit -= amt
            pipe.multi()
            pipe.set(user_id, msgpack.encode(user_obj))
            pipe.execute()
        except redis.exceptions.WatchError:
            abort(400, "Concurrent update error. Please retry")
        except redis.exceptions.RedisError:
            abort(400, DB_ERROR_STR)

    try:
        db.hset(f"txn:{txn_id}:payment", mapping={"user_id": user_id, "amount": amt})
    except redis.exceptions.RedisError:
        abort(400, DB_ERROR_STR)

    publish_event({"event_type": "payment_prepared", "txn_id": txn_id, "user_id": user_id, "amount": amt})
    return Response("Payment prepared", status=200)
# -------------------------------------------------------------------

@app.post('/pay/<user_id>/<amount>')
def remove_credit(user_id: str, amount: int):
    app.logger.debug(f"Removing {amount} credit from user: {user_id}")
    user_entry: UserValue = get_user_from_db(user_id)
    new_amount = int(amount)
    user_entry.credit -= new_amount
    if user_entry.credit < 0:
        abort(400, f"User: {user_id} credit cannot get reduced below zero!")
    try:
        db.set(user_id, msgpack.encode(user_entry))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    publish_event({"event_type": "payment_made", "user_id": user_id, "amount": new_amount})
    return Response(f"User: {user_id} credit updated to: {user_entry.credit}", status=200)

@app.post("/commit/<txn_id>")
def commit_payment(txn_id: str):
    key = f"txn:{txn_id}:payment"
    try:
        db.delete(key)
    except redis.exceptions.RedisError:
        abort(400, DB_ERROR_STR)
    publish_event({"event_type": "payment_committed", "txn_id": txn_id})
    return Response("Payment commit successful", status=200)

@app.post("/abort/<txn_id>")
def abort_payment(txn_id: str):
    key = f"txn:{txn_id}:payment"
    try:
        data = db.hgetall(key)
        if not data:
            return Response("No payment reservation found", status=200)
        user_id = data[b'user_id'].decode()
        amount = int(data[b'amount'].decode())
        with db.pipeline() as pipe:
            try:
                pipe.watch(user_id)
                current_data = db.get(user_id)
                if current_data is None:
                    pipe.unwatch()
                    abort(400, f"User: {user_id} not found!")
                user_obj: UserValue = msgpack.decode(current_data, type=UserValue)
                user_obj.credit += amount
                pipe.multi()
                pipe.set(user_id, msgpack.encode(user_obj))
                pipe.execute()
            except redis.exceptions.WatchError:
                abort(400, "Concurrent update error during abort. Please retry")
            except redis.exceptions.RedisError:
                abort(400, DB_ERROR_STR)
        db.delete(key)
    except redis.exceptions.RedisError:
        abort(400, DB_ERROR_STR)
    publish_event({"event_type": "payment_aborted", "txn_id": txn_id})
    return Response("Payment abort successful", status=200)

if __name__ == '__main__':
    listener_thread = threading.Thread(target=listen_to_events, args=("events",), daemon=True)
    listener_thread.start()
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
