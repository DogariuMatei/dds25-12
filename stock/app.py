import copy
import logging
import os
import atexit
import uuid
from collections import defaultdict

import redis

from msgspec import msgpack, Struct
from flask import Flask, jsonify, abort, Response
from confluent_kafka import Consumer, KafkaException, Producer
from threading import Thread

from redis import WatchError
from redis.client import Pipeline

DB_ERROR_STR = "DB error"
app = Flask("stock-service")
db: redis.Redis = redis.Redis(host=os.environ['REDIS_HOST'],
                              port=int(os.environ['REDIS_PORT']),
                              password=os.environ['REDIS_PASSWORD'],
                              db=int(os.environ['REDIS_DB']))
default_producer_config = {'bootstrap.servers': 'kafka:9092'}
default_consumer_config = {
    'bootstrap.servers': 'kafka:9092',
    # 'auto.offset.reset': 'earliest', #'smallest'
    'auto.offset.reset': 'latest', #'smallest'
    'enable.auto.commit': 'false',
    # 'enable.auto.offset.store': 'false'
}
producer = Producer(**default_producer_config)

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
    event_type: str
    event_id: str

def get_item_from_db(item_id: str, redis_client: redis.client.Redis) -> StockValue | None:
    try:
        entry: bytes = redis_client.get(item_id)
    except WatchError as e: raise e
    except redis.exceptions.RedisError:
        abort(400, DB_ERROR_STR)
    entry: StockValue | None = msgpack.decode(entry, type=StockValue) if entry else None
    if entry is None:
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
        abort(400, DB_ERROR_STR)
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
        abort(400, DB_ERROR_STR)
    return jsonify({"msg": "Batch init for stock successful"})

@app.get('/find/<item_id>')
def find_item(item_id: str):
    item_entry: StockValue = get_item_from_db(item_id, db)
    return jsonify(
        {
            "stock": item_entry.stock,
            "price": item_entry.price
        }
    )

@app.post('/add/<item_id>/<amount>')
def add_stock(item_id: str, amount: int):
    with db.pipeline() as pipe:
        while True:
            try:
                pipe.watch(item_id)
                item_entry: StockValue = get_item_from_db(item_id, pipe)
                item_entry.stock += int(amount)
                pipe.multi()
                try:
                    pipe.set(item_id, msgpack.encode(item_entry))
                except WatchError as e:
                    raise e
                except redis.exceptions.RedisError:
                    abort(400, DB_ERROR_STR)
                pipe.execute()
                break
            except WatchError:
                continue
    return Response(f"Item: {item_id} stock updated to: ", status=200) #{item_entry.stock}


@app.post('/subtract/<item_id>/<amount>')
def remove_stock(item_id: str, amount: int):
    with db.pipeline() as pipe:
        while True:
            try:
                pipe.watch(item_id)
                item_entry: StockValue = get_item_from_db(item_id, pipe)
                item_entry.stock -= int(amount)
                app.logger.debug(f"Item: {item_id} stock updated to: {item_entry.stock}")
                if item_entry.stock < 0:
                    abort(400, f"Item: {item_id} stock cannot get reduced below zero!")
                pipe.multi()
                try:
                    pipe.set(item_id, msgpack.encode(item_entry))
                except WatchError as e:
                    raise e
                except redis.exceptions.RedisError:
                    abort(400, DB_ERROR_STR)
                pipe.execute()
                break
            except WatchError:
                continue
    return Response(f"Item: {item_id} stock updated to: ", status=200) #{item_entry.stock}

def produce_stock_event(order_entry: OrderValue, event_type: str):
    new_order_entry = copy.deepcopy(order_entry)
    new_order_entry.event_type = event_type
    producer.produce('stock', key=new_order_entry.order_id, value=msgpack.encode(new_order_entry))
    producer.flush()

def consumer_executor(consumer: Consumer, execution_function):
    try:
        while True:
            try:
                msg = consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    raise KafkaException(msg.error())
                execution_function(msg)
                consumer.commit()
            except Exception as e:
                app.logger.error(e)
    finally:
        consumer.close()

def start_order_consumer(): # TODO not only created.
    order_consumer = Consumer(default_consumer_config | {
        'group.id': 'stock',
    })
    order_consumer.subscribe(['order'])

    def execute_function(msg):
        order_entry = msgpack.decode(msg.value(), type=OrderValue)
        app.logger.info(f"Processing order event: {order_entry.event_type}")
        items_quantities: dict[str, int] = defaultdict(int)
        for item_id, quantity in order_entry.items:
            items_quantities[item_id] += quantity
        logs_id = f"logs{order_entry.event_id}"
        log_type = db.get(logs_id)
        if log_type:
            return
        app.logger.info(f"Trying to reserve stock for order {order_entry.order_id}")
        with (db.pipeline() as pipe):
            while True:
                try:
                    # TODO locks stock ids. Potentially many conflicts with competing orders. Consider producing events for each item? But loses order id partition key (seq. order).
                    #  Locking also inefficient if items are sharded. Parallelism on orders topic is also problematic
                    #  should we consider that all stock fit in single db?
                    pipe.watch(*items_quantities.keys())
                    entries = []
                    for item_id, amount in items_quantities.items():
                        entry: bytes = pipe.get(item_id)
                        entry: StockValue | None = msgpack.decode(entry, type=StockValue) if entry else None
                        if entry is None:
                            produce_stock_event(order_entry, "FAIL")
                            return
                        entry.stock -= int(amount)
                        if entry.stock < 0:
                            app.logger.error(f"Not enough stock for item {item_id}: requested {amount}, available {entry.stock + amount}")
                            produce_stock_event(order_entry, "FAIL")
                            return
                        app.logger.info(f"Confirmed reservation for item {item_id}, reduced stock by {amount}")
                        entries.append((item_id, entry))
                    pipe.multi()
                    for item_id, entry in entries:
                        pipe.set(item_id, msgpack.encode(entry))
                    pipe.set(logs_id, "SUBTRACTED")
                    pipe.execute()
                    break
                except WatchError as e:
                    app.logger.error(f"Error reserving item : {e}")
                    continue
        app.logger.info(f"Successfully reserved all items for order {order_entry.order_id}")
        produce_stock_event(order_entry, "SUCCESS")

    consumer_executor(order_consumer, execute_function)

def start_payment_consumer():
    payment_consumer = Consumer(default_consumer_config | {
        'group.id': 'stock',
    })
    payment_consumer.subscribe(['payment'])
    app.logger.info(f"Starting payment events consumer ")

    def execute_function(msg):
        order_entry = msgpack.decode(msg.value(), type=OrderValue)
        app.logger.info(f"Processing payment event: {order_entry.event_type}")
        if order_entry.event_type != 'FAIL':
            return
        app.logger.info(f"Payment failed for order {order_entry.order_id}, releasing stock")
        app.logger.warning(f"Rolling back reservations for order {order_entry.order_id} {order_entry.event_id}")
        items_quantities: dict[str, int] = defaultdict(int)
        for item_id, quantity in order_entry.items:
            items_quantities[item_id] += quantity
        logs_id = f"logs{order_entry.event_id}"
        log_type = db.get(logs_id)
        app.logger.info(f"{order_entry.event_id} {log_type} {log_type != "SUBTRACTED"}")
        if log_type != b"SUBTRACTED":
            return

        with (db.pipeline() as pipe):
            while True:
                try:
                    pipe.watch(*items_quantities.keys())
                    entries = []
                    for item_id, amount in items_quantities.items():
                        entry: bytes = pipe.get(item_id)
                        entry: StockValue = msgpack.decode(entry, type=StockValue)
                        entry.stock += int(amount)
                        app.logger.info(f"Released reservation for item {item_id}, amount {amount}")
                        entries.append((item_id, entry))
                    pipe.multi()
                    for item_id, entry in entries:
                        pipe.set(item_id, msgpack.encode(entry))
                    pipe.set(logs_id, "ADDED")
                    pipe.execute()
                    break
                except WatchError as e:
                    app.logger.error(f"Error processing payment: {e}")
                    continue
        produce_stock_event(order_entry, "FAIL") #for consistency

    consumer_executor(payment_consumer, execute_function)

def launch_consumers():
   app.logger.info("Initializing stock service event processing")
   order_thread = Thread(target=start_order_consumer)#, daemon=True)
   order_thread.start()
   payment_thread = Thread(target=start_payment_consumer)#, daemon=True)
   payment_thread.start()

launch_consumers()

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)