import logging
import os
import atexit
import uuid

import redis

from msgspec import msgpack, Struct
from flask import Flask, jsonify, abort, Response

DB_ERROR_STR = "DB error"


app = Flask("payment-service")

db: redis.Redis = redis.Redis(host=os.environ['REDIS_HOST'],
                              port=int(os.environ['REDIS_PORT']),
                              password=os.environ['REDIS_PASSWORD'],
                              db=int(os.environ['REDIS_DB']))


def close_db_connection():
    db.close()


atexit.register(close_db_connection)


class UserValue(Struct):
    credit: int


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
def remove_credit(user_id: str, amount: int):
    app.logger.debug(f"Removing {amount} credit from user: {user_id}")
    user_entry: UserValue = get_user_from_db(user_id)
    # update credit, serialize and update database
    user_entry.credit -= int(amount)
    if user_entry.credit < 0:
        abort(400, f"User: {user_id} credit cannot get reduced below zero!")
    try:
        db.set(user_id, msgpack.encode(user_entry))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(f"User: {user_id} credit updated to: {user_entry.credit}", status=200)


@app.post("/prepare_pay/<txn_id>/<user_id>/<amount>")
def prepare_pay(txn_id: str, user_id: str, amount: int):
    user = get_user_from_db(user_id)
    amt = int(amount)
    # print(f"prepare {amt}")

    if user.credit < amt:
        # print(f"Not enough credit")
        abort(400, "Not enough credit")

    user.credit -= amt
    if user.credit < 0:
        # print(f"User credit cannot be negative")
        abort(400, "User credit cannot be negative")

    try:
        db.set(user_id, msgpack.encode(user))
    except redis.exceptions.RedisError:
        print(f"ERROR: {DB_ERROR_STR}")
        abort(400, DB_ERROR_STR)

    try:
        db.hset(f"txn:{txn_id}:payment", mapping={"user_id": user_id, "amount": amt})
    except redis.exceptions.RedisError:
        print(f"ERROR: {DB_ERROR_STR}")
        abort(400, DB_ERROR_STR)

    return Response("Payment prepared", status=200)

@app.post("/commit/<txn_id>")
def commit_payment(txn_id: str):

    key = f"txn:{txn_id}:payment"
    try:
        db.delete(key)
    except redis.exceptions.RedisError:
        print(f"ERROR COMMIT PAY: {DB_ERROR_STR}")
        abort(400, DB_ERROR_STR)

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

        user = get_user_from_db(user_id)
        user.credit += amount
        db.set(user_id, msgpack.encode(user))

        db.delete(key)

    except redis.exceptions.RedisError:
        print(f"ERROR ABORT PAY: {DB_ERROR_STR}")
        abort(400, DB_ERROR_STR)

    return Response("Payment abort successful", status=200)





if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
