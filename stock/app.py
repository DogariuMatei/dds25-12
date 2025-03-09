import logging
import os
import atexit
import uuid
from collections import defaultdict

import redis

from msgspec import msgpack, Struct
from flask import Flask, jsonify, abort, Response
from confluent_kafka import Consumer, KafkaException
from threading import Thread

from redis.client import Pipeline

DB_ERROR_STR = "DB error"

app = Flask("stock-service")

db: redis.Redis = redis.Redis(host=os.environ['REDIS_HOST'],
                              port=int(os.environ['REDIS_PORT']),
                              password=os.environ['REDIS_PASSWORD'],
                              db=int(os.environ['REDIS_DB']))
consumer = Consumer({
    'bootstrap.servers': 'kafka:9092',
    'group.id': 'order',
    'auto.offset.reset': 'earliest', #'smallest'
    'enable.auto.commit': 'true',
    'enable.auto.offset.store': 'false'
}) #TODO retries
consumer.subscribe(['order'])


def close_db_connection():
    db.close()


atexit.register(close_db_connection)


class StockValue(Struct):
    stock: int
    price: int

class OrderValue(Struct):
    order_id: str
    paid: bool
    items: list[tuple[str, int]]
    user_id: str
    total_cost: int

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

def start_consumer():
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())
            order_entry = msgpack.decode(msg.value(), type=OrderValue)
            items_quantities: dict[str, int] = defaultdict(int)
            for item_id, quantity in order_entry.items:
                items_quantities[item_id] += quantity
            logs_id = f"logs{order_entry.order_id}"

            def watched_sequence(pipe: Pipeline):
                log_type = pipe.get(logs_id)
                if not log_type:
                    entries = []
                    for item_id, amount in items_quantities.items():
                        entry: bytes = pipe.get(item_id)
                        entry: StockValue | None = msgpack.decode(entry, type=StockValue) if entry else None
                        if entry is None:
                            # TODO stock_failed
                            return
                        entry.stock -= int(amount)
                        if entry.stock < 0:
                            # TODO stock_failed
                            return
                        entries.append((item_id, entry))
                    pipe.multi()
                    for item_id, entry in entries:
                        pipe.set(item_id, msgpack.encode(entry))
                    pipe.set(logs_id, "SUBTRACTED") #TODO store set of types?

            # TODO locks stock ids. Potentially many conflicts with competing orders. Consider producing events for each item? But loses order id partition key (seq. order).
            #  Locking also inefficient if items are sharded. Parallelism on orders topic is also problematic
            db.transaction(watched_sequence, *items_quantities.keys())
    finally:
        consumer.close()

def launch_consumer():
   t = Thread(target=start_consumer) #TODO fix logging
   t.start()

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)

launch_consumer()