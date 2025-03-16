import copy
import logging
import os
import atexit
import random
import time
import uuid
from collections import defaultdict
from threading import Thread

import redis
import requests

from msgspec import msgpack, Struct
from flask import Flask, jsonify, abort, Response
from confluent_kafka import Producer, Consumer, KafkaException
from redis import WatchError
from redis.client import Pipeline

DB_ERROR_STR = "DB error"
REQ_ERROR_STR = "Requests error"
GATEWAY_URL = os.environ['GATEWAY_URL']
app = Flask("order-service")
db: redis.Redis = redis.Redis(host=os.environ['REDIS_HOST'],
                              port=int(os.environ['REDIS_PORT']),
                              password=os.environ['REDIS_PASSWORD'],
                              db=int(os.environ['REDIS_DB']))
event_db: redis.Redis = redis.Redis(host=os.environ['EVENT_REDIS_HOST'],
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
producer = Producer(default_producer_config)

def close_db_connection():
    db.close()

atexit.register(close_db_connection)

class OrderValue(Struct):
    paid: bool
    items: list[tuple[str, int]]
    user_id: str
    total_cost: int

class OrderDto(Struct):
    order_id: str
    paid: bool
    items: list[tuple[str, int]]
    user_id: str
    total_cost: int
    event_type: str
    event_id: str

def get_order_from_db(order_id: str) -> OrderValue | None:
    try:
        entry: bytes = db.get(order_id)
    except redis.exceptions.RedisError:
        abort(400, DB_ERROR_STR)
    entry: OrderValue | None = msgpack.decode(entry, type=OrderValue) if entry else None
    if entry is None:
        abort(400, f"Order: {order_id} not found!")
    return entry

@app.post('/create/<user_id>')
def create_order(user_id: str):
    key = str(uuid.uuid4())
    value = msgpack.encode(OrderValue(paid=False, items=[], user_id=user_id, total_cost=0))
    try:
        db.set(key, value)
    except redis.exceptions.RedisError:
        abort(400, DB_ERROR_STR)
    return jsonify({'order_id': key})

@app.post('/batch_init/<n>/<n_items>/<n_users>/<item_price>')
def batch_init_users(n: int, n_items: int, n_users: int, item_price: int):
    n = int(n)
    n_items = int(n_items)
    n_users = int(n_users)
    item_price = int(item_price)

    def generate_entry() -> OrderValue:
        user_id = random.randint(0, n_users - 1)
        item1_id = random.randint(0, n_items - 1)
        item2_id = random.randint(0, n_items - 1)
        value = OrderValue(paid=False,
                           items=[(f"{item1_id}", 1), (f"{item2_id}", 1)],
                           user_id=f"{user_id}",
                           total_cost=2*item_price)
        return value

    kv_pairs: dict[str, bytes] = {f"{i}": msgpack.encode(generate_entry())
                                  for i in range(n)}
    try:
        db.mset(kv_pairs)
    except redis.exceptions.RedisError:
        abort(400, DB_ERROR_STR)
    return jsonify({"msg": "Batch init for orders successful"})

def order_dto(order_id: str, order_entry: OrderValue):
    return {
            "order_id": order_id,
            "paid": order_entry.paid,
            "items": order_entry.items,
            "user_id": order_entry.user_id,
            "total_cost": order_entry.total_cost
        }

@app.get('/find/<order_id>')
def find_order(order_id: str):
    order_entry: OrderValue = get_order_from_db(order_id)
    return jsonify(order_dto(order_id, order_entry))

def send_post_request(url: str):
    try:
        response = requests.post(url)
    except requests.exceptions.RequestException:
        abort(400, REQ_ERROR_STR)
    else:
        return response

def send_get_request(url: str):
    try:
        response = requests.get(url)
    except requests.exceptions.RequestException:
        abort(400, REQ_ERROR_STR)
    else:
        return response

@app.post('/addItem/<order_id>/<item_id>/<quantity>') # TODO ASSUMPTION (according to benchmark, test): no items are added to order after first attempt to checkout. No items are added to paid order.
def add_item(order_id: str, item_id: str, quantity: int):
    order_entry: OrderValue = get_order_from_db(order_id)
    item_reply = send_get_request(f"{GATEWAY_URL}/stock/find/{item_id}")
    if item_reply.status_code != 200:
        abort(400, f"Item: {item_id} does not exist!")
    item_json: dict = item_reply.json()
    order_entry.items.append((item_id, int(quantity)))
    order_entry.total_cost += int(quantity) * item_json["price"]
    try:
        db.set(order_id, msgpack.encode(order_entry))
    except redis.exceptions.RedisError:
        abort(400, DB_ERROR_STR)
    return Response(f"Item: {item_id} added to: {order_id} price updated to: {order_entry.total_cost}",
                    status=200)

def produce_order_event(order_entry: OrderValue, order_id: str, event_id: str):
    producer.produce('order', key=order_id, value=msgpack.encode(order_dto(order_id, order_entry) | { "event_type": "CREATED", "event_id": event_id }))
    producer.flush()

def produce_result(order_entry: OrderDto, event_type: str):
    new_order_entry = copy.deepcopy(order_entry)
    new_order_entry.event_type = event_type
    event_db.xadd(new_order_entry.event_id, { b'result': msgpack.encode(new_order_entry) })
    # event_db.publish(order_entry.event_id, msgpack.encode(order_entry))

@app.post('/checkout/<order_id>')
def checkout(order_id: str): # TODO can same order be attempted to checkout twice?
    app.logger.debug(f"Checking out {order_id}")
    order_entry: OrderValue = get_order_from_db(order_id)
    # abort(400, "User out of credit")
    order_entry.paid = True
    event_id = str(uuid.uuid4())
    # pubsub = event_db.pubsub()
    # pubsub.subscribe(event_id)
    produce_order_event(order_entry, order_id, event_id)
    # for message in pubsub.listen():
    #     if message['type'] == 'message':
    #         order_entry = msgpack.decode(message['data'], type=OrderDto)
    #         if order_entry.event_type != "SUCCESS":
    #             event_db.delete(event_id)
    #             abort(400)
    #         app.logger.debug("Checkout successful")
    #         event_db.delete(event_id)
    #         return Response("Checkout successful", status=200)
    while True:
        messages = event_db.xread({event_id: '0'}, count=1)
        if messages:
            stream_name, stream_messages = messages[0]
            message_id, message_data = stream_messages[0]
            order_entry = msgpack.decode(message_data[b'result'], type=OrderDto)
            if order_entry.event_type != "SUCCESS":
                # event_db.delete(event_id)
                abort(400)
            app.logger.debug("Checkout successful")
            # event_db.delete(event_id)
            return Response("Checkout successful", status=200)
        time.sleep(0.001)

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

def start_payment_consumer():
    payment_consumer = Consumer(default_consumer_config | {
        'group.id': 'order',
    })
    payment_consumer.subscribe(['payment'])

    def execution_function(msg):
        order_entry = msgpack.decode(msg.value(), type=OrderDto)
        logs_id = f"logs{order_entry.event_id}"
        if order_entry.event_type == "SUCCESS":
            log_type = db.get(logs_id)
            if not log_type:
                with (db.pipeline() as pipe):
                    while True:
                        try:
                            pipe.multi()
                            pipe.set(order_entry.order_id, msgpack.encode(
                                OrderValue(paid=order_entry.paid, items=order_entry.items, user_id=order_entry.user_id,
                                           total_cost=order_entry.total_cost)))
                            pipe.set(logs_id, "SUCCESS")
                            pipe.execute()
                            break
                        except WatchError as e:
                            app.logger.error(f"Error processing payment: {e}")
                            continue
                produce_result(order_entry, "SUCCESS")
        # else:
        #     produce_result(order_entry, "FAIL") #eventual consistent

    consumer_executor(payment_consumer, execution_function)

def start_stock_consumer():
    stock_consumer = Consumer(default_consumer_config | {
        'group.id': 'order',
    })
    stock_consumer.subscribe(['stock'])

    def execution_function(msg):
        order_entry = msgpack.decode(msg.value(), type=OrderDto)
        if order_entry.event_type == "FAIL":
            produce_result(order_entry, "FAIL")

    consumer_executor(stock_consumer, execution_function)

def launch_consumers():
   payment_thread = Thread(target=start_payment_consumer)#, daemon=True)
   payment_thread.start()
   stock_thread = Thread(target=start_stock_consumer)#, daemon=True)
   stock_thread.start()

launch_consumers()

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)