import copy
import logging
import os
import atexit
import uuid
from threading import Thread

import redis
from confluent_kafka import Consumer, KafkaException, Producer

from msgspec import msgpack, Struct
from flask import Flask, jsonify, abort, Response
from redis import WatchError
from redis.client import Pipeline

DB_ERROR_STR = "DB error"
app = Flask("payment-service")
db: redis.Redis = redis.Redis(host=os.environ['REDIS_HOST'],
                              port=int(os.environ['REDIS_PORT']),
                              password=os.environ['REDIS_PASSWORD'],
                              db=int(os.environ['REDIS_DB']))
default_consumer_config = {
    'bootstrap.servers': 'kafka:9092',
    # 'auto.offset.reset': 'earliest', #'smallest'
    'auto.offset.reset': 'latest', #'smallest'
    'enable.auto.commit': 'false',
    # 'enable.auto.offset.store': 'false'
}
default_producer_config = {'bootstrap.servers': 'kafka:9092'}
producer = Producer(default_producer_config)

def close_db_connection():
    db.close()

atexit.register(close_db_connection)

class UserValue(Struct):
    credit: int

class OrderValue(Struct):
    order_id: str
    paid: bool
    items: list[tuple[str, int]]
    user_id: str
    total_cost: int
    event_type: str
    event_id: str

def get_user_from_db(user_id: str, redis_client: redis.client.Redis) -> UserValue | None:
    try:
        entry: bytes = redis_client.get(user_id)
    except WatchError as e: raise e
    except redis.exceptions.RedisError:
        abort(400, DB_ERROR_STR)
    entry: UserValue | None = msgpack.decode(entry, type=UserValue) if entry else None
    if entry is None:
        abort(400, f"User: {user_id} not found!")
    return entry

@app.post('/create_user')
def create_user():
    key = str(uuid.uuid4())
    value = msgpack.encode(UserValue(credit=0))
    try:
        db.set(key, value)
    except redis.exceptions.RedisError:
        abort(400, DB_ERROR_STR)
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
    with db.pipeline() as pipe:
        while True:
            try:
                pipe.watch(user_id)
                user_entry: UserValue = get_user_from_db(user_id, pipe)
                user_entry.credit += int(amount)
                pipe.multi()
                try:
                    pipe.set(user_id, msgpack.encode(user_entry))
                except WatchError as e:
                    raise e
                except redis.exceptions.RedisError:
                    abort(400, DB_ERROR_STR)
                pipe.execute()
                break
            except WatchError:
                continue
    return Response(f"User: {user_id} credit updated to: ", status=200) #{user_entry.credit}

@app.post('/pay/<user_id>/<amount>')
def remove_credit(user_id: str, amount: int):
    app.logger.debug(f"Removing {amount} credit from user: {user_id}")
    with db.pipeline() as pipe:
        while True:
            try:
                pipe.watch(user_id)
                user_entry: UserValue = get_user_from_db(user_id, pipe)
                user_entry.credit -= int(amount)
                if user_entry.credit < 0:
                    abort(400, f"User: {user_id} credit cannot get reduced below zero!")
                pipe.multi()
                try:
                    pipe.set(user_id, msgpack.encode(user_entry))
                except WatchError as e:
                    raise e
                except redis.exceptions.RedisError:
                    abort(400, DB_ERROR_STR)
                pipe.execute()
                break
            except WatchError:
                continue
    return Response(f"User: {user_id} credit updated to: ", status=200) #{user_entry.credit}

def produce_payment_event(order_entry: OrderValue, event_type: str):
    new_order_entry = copy.deepcopy(order_entry)
    new_order_entry.event_type = event_type
    producer.produce('payment', key=new_order_entry.order_id, value=msgpack.encode(new_order_entry))
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

def start_stock_consumer():
    stock_consumer = Consumer(default_consumer_config | {
        'group.id': 'payment',
    })
    stock_consumer.subscribe(['stock'])
    app.logger.info(f"Starting stock events consumer ")

    def execute_function(msg):
        order_entry = msgpack.decode(msg.value(), type=OrderValue)
        if order_entry.event_type != 'SUCCESS':
            return
        app.logger.info(f"Processing stock event: {order_entry.event_type}")
        logs_id = f"logs{order_entry.event_id}"
        user_id = order_entry.user_id
        log_type = db.get(logs_id)
        if log_type:
            return
        app.logger.info(f"Processing payment for order {order_entry.order_id}, amount {order_entry.total_cost}")
        with (db.pipeline() as pipe):
            while True:
                try:
                    pipe.watch(user_id)
                    entry: bytes = pipe.get(user_id)
                    entry: UserValue | None = msgpack.decode(entry, type=UserValue) if entry else None
                    if entry is None:
                        produce_payment_event(order_entry, "FAIL")
                        return
                    entry.credit -= int(order_entry.total_cost)
                    if entry.credit < 0:
                        produce_payment_event(order_entry, "FAIL")
                        return
                    pipe.multi()
                    pipe.set(user_id, msgpack.encode(entry))
                    pipe.set(logs_id, "SUBTRACTED")
                    pipe.execute()
                    break
                except WatchError as e:
                    app.logger.error(f"Error processing payment: {e}")
                    continue
        produce_payment_event(order_entry, "SUCCESS")

    consumer_executor(stock_consumer, execute_function)

def launch_consumers():
   stock_thread = Thread(target=start_stock_consumer)#, daemon=True)
   stock_thread.start()

launch_consumers()

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)