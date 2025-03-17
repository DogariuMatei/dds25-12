import logging
import os
import atexit
import uuid

import redis

from msgspec import msgpack, Struct
from flask import Flask, jsonify, abort, Response


DB_ERROR_STR = "DB error"

app = Flask("stock-service")

db: redis.Redis = redis.Redis(host=os.environ['REDIS_HOST'],
                              port=int(os.environ['REDIS_PORT']),
                              password=os.environ['REDIS_PASSWORD'],
                              db=int(os.environ['REDIS_DB']))


def close_db_connection():
    db.close()


atexit.register(close_db_connection)


class StockValue(Struct):
    stock: int
    price: int


def get_item_from_db(item_id: str) -> StockValue | None:
    # get serialized data
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
    app.logger.debug(f"Item: {key} created")
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
            "price": item_entry.price
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

    item_entry.stock -= int(amount)
    app.logger.debug(f"Item: {item_id} stock updated to: {item_entry.stock}")
    if item_entry.stock < 0:
        abort(400, f"Item: {item_id} stock cannot get reduced below zero!")
    try:
        db.set(item_id, msgpack.encode(item_entry))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(f"Item: {item_id} stock updated to: {item_entry.stock}", status=200)


@app.post('/prepare_subtract/<txn_id>/<item_id>/<amount>')
def prepare_subtract(txn_id: str, item_id: str, amount: int):

    item_entry = get_item_from_db(item_id)
    amt = int(amount)

    if item_entry.stock < amt:
        # print(f"Not enough stock on item {item_id}")
        abort(400, f"Not enough stock on item {item_id}")

    item_entry.stock -= amt
    if item_entry.stock < 0:
        # print(f"Item stock cannot be negative")
        abort(400, "Item stock cannot be negative")

    try:
        db.set(item_id, msgpack.encode(item_entry))
    except redis.exceptions.RedisError:
        print(f"ERROR: {DB_ERROR_STR}")
        abort(400, DB_ERROR_STR)

    try:
        db.hset(f"txn:{txn_id}:stock", item_id, amt)
    except redis.exceptions.RedisError:
        print(f"ERROR: {DB_ERROR_STR}")
        abort(400, DB_ERROR_STR)

    return Response(f"Stock reserved for item {item_id}: {amt}", status=200)

@app.post('/commit/<txn_id>')
def commit_stock(txn_id: str):

    reservation_key = f"txn:{txn_id}:stock"
    try:
        db.delete(reservation_key)
    except redis.exceptions.RedisError:
        print(f"ERROR COMMIT STOCK: {DB_ERROR_STR}")
        abort(400, DB_ERROR_STR)

    return Response("Stock commit successful", status=200)

@app.post('/abort/<txn_id>')
def abort_stock(txn_id: str):

    reservation_key = f"txn:{txn_id}:stock"

    try:
        items_reserved = db.hgetall(reservation_key)
        for (item_id_bytes, amt_bytes) in items_reserved.items():
            item_id_str = item_id_bytes.decode()
            amt_int = int(amt_bytes.decode())

            item_entry = get_item_from_db(item_id_str)
            item_entry.stock += amt_int
            db.set(item_id_str, msgpack.encode(item_entry))

        db.delete(reservation_key)
    except redis.exceptions.RedisError:
        print(f"ERROR ABORT STOCK: {DB_ERROR_STR}")
        abort(400, DB_ERROR_STR)

    return Response("Stock abort completed", status=200)





if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
