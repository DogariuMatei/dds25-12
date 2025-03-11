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
ORDER_EVENTS = "order:events"
STOCK_EVENTS = "stock:events"
PAYMENT_EVENTS = "payment:events"

# Event types
ORDER_CREATED = "order.created" # we consume
STOCK_RESERVED = "stock.reserved" # we post
STOCK_FAILED = "stock.failed" # we post
STOCK_RELEASED = "stock.released" # we post
PAYMENT_FAILED = "payment.failed" # we consume
PAYMENT_SUCCEEDED = "payment.succeeded" # we consume

# Consumer groups
STOCK_ORDER_GROUP = "stock-order-consumers"
STOCK_PAYMENT_GROUP = "stock-payment-consumers"

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
    reserved: int = 0


# Helper functions for Redis Streams
def publish_event(stream, event_type, data):
    """Publish an event to a Redis Stream"""
    event = {
        "type": event_type,
        "data": data,
        "timestamp": int(time.time() * 1000),
        "transaction_id": data.get("transaction_id")
    }
    try:
        db.xadd(stream, {b'event': json.dumps(event).encode()})
        app.logger.debug(f"Published event {event_type} to {stream}")
        return event
    except Exception as e:
        app.logger.error(f"Failed to publish event: {e}")
        return None


def ensure_consumer_group(stream, group):
    """Create a consumer group if it doesn't exist"""
    try:
        db.xgroup_create(stream, group, id='0-0', mkstream=True)
        app.logger.info(f"Created consumer group {group} for stream {stream}")
    except redis.exceptions.ResponseError as e:
        if 'BUSYGROUP' in str(e):  # Group already exists
            app.logger.debug(f"Consumer group {group} already exists for stream {stream}")
            pass
        else:
            app.logger.error(f"Error creating consumer group: {e}")
            raise


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
    value = msgpack.encode(StockValue(stock=0, price=int(price), reserved=0))
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
    kv_pairs: dict[str, bytes] = {f"{i}": msgpack.encode(StockValue(stock=starting_stock, price=item_price, reserved=0))
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
            "price": item_entry.price,
            "reserved": item_entry.reserved,
            "available": item_entry.stock - item_entry.reserved
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
    lock_key = f"lock:stock:{item_id}"
    lock_id = str(uuid.uuid4())

    # Try to acquire lock
    lock_acquired = db.set(lock_key, lock_id, nx=True, ex=10)
    if not lock_acquired:
        return abort(409, f"Item {item_id} is currently being modified by another process")

    try:
        item_entry: StockValue = get_item_from_db(item_id)
        amount = int(amount)

        # Check if we have enough available stock (accounting for reservations)
        if item_entry.stock - item_entry.reserved < amount:
            return abort(400, f"Item: {item_id} does not have enough available stock!")

        # Reserve the stock rather than immediately subtracting
        item_entry.reserved += amount
        app.logger.debug(f"Item: {item_id} stock reserved: {item_entry.reserved}")

        try:
            db.set(item_id, msgpack.encode(item_entry))

        except redis.exceptions.RedisError:
            return abort(400, DB_ERROR_STR)

        return Response(f"Item: {item_id} stock reserved. Available: {item_entry.stock - item_entry.reserved}",
                        status=200)
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


def make_reservation(item_id, amount):
    lock_key = f"lock:stock:{item_id}"
    lock_id = str(uuid.uuid4())

    # Get Lock
    lock_acquired = db.set(lock_key, lock_id, nx=True, ex=10)
    if not lock_acquired:
        app.logger.error(f"Could not acquire lock for item {item_id}")
        return False

    try:
        item_entry = get_item_from_db(item_id)

        # Check stock availability
        if item_entry.stock - item_entry.reserved < amount:
            app.logger.error(
                f"Not enough stock for item {item_id}: requested {amount}, available {item_entry.stock - item_entry.reserved}")
            return False

        # Reserve the stock
        item_entry.reserved += amount
        db.set(item_id, msgpack.encode(item_entry))
        app.logger.debug(f"Reserved {amount} units of item {item_id}")

    finally:
        # Release the lock
        script = """
                    if redis.call('get', KEYS[1]) == ARGV[1] then
                        return redis.call('del', KEYS[1])
                    else
                        return 0
                    end
                 """
        db.eval(script, 1, lock_key, lock_id)

def release_reservation(item_id, amount):
    """Release a previously made reservation"""
    lock_key = f"lock:stock:{item_id}"
    lock_id = str(uuid.uuid4())

    # Try to acquire lock
    lock_acquired = db.set(lock_key, lock_id, nx=True, ex=10)
    if not lock_acquired:
        app.logger.error(f"Could not acquire lock for releasing reservation on item {item_id}")
        return False

    try:
        item_entry: StockValue = get_item_from_db(item_id)
        amount = int(amount)

        if item_entry.reserved < amount:
            app.logger.warning(f"Attempting to release more than is reserved for item {item_id}")
            item_entry.reserved = 0
        else:
            item_entry.reserved -= amount

        try:
            db.set(item_id, msgpack.encode(item_entry))
            app.logger.info(f"Released reservation for item {item_id}, amount {amount}")

            # # Publish stock released event
            # publish_event(STOCK_EVENTS, STOCK_RELEASED, {
            #     "item_id": item_id,
            #     "amount": amount,
            #     "remaining_reserved": item_entry.reserved,
            #     "available": item_entry.stock - item_entry.reserved
            # })

            return True

        except redis.exceptions.RedisError as e:
            app.logger.error(f"DB error when releasing reservation: {e}")
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

def confirm_reservation(item_id, amount):
    """Confirm a reservation by actually reducing the stock"""
    lock_key = f"lock:stock:{item_id}"
    lock_id = str(uuid.uuid4())

    # Try to acquire lock
    lock_acquired = db.set(lock_key, lock_id, nx=True, ex=10)
    if not lock_acquired:
        app.logger.error(f"Could not acquire lock for confirming reservation on item {item_id}")
        return False

    try:
        item_entry: StockValue = get_item_from_db(item_id)
        amount = int(amount)

        if item_entry.reserved < amount:
            app.logger.error(f"Not enough reserved stock for item {item_id}")
            return False

        # Actually subtract from stock and reduce reservation
        item_entry.stock -= amount
        item_entry.reserved -= amount

        try:
            db.set(item_id, msgpack.encode(item_entry))
            app.logger.info(f"Confirmed reservation for item {item_id}, reduced stock by {amount}")
            return True
        except redis.exceptions.RedisError as e:
            app.logger.error(f"DB error when confirming reservation: {e}")
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


def process_order_events():
    """Process events from the order stream"""
    consumer_name = f"stock-consumer-{uuid.uuid4()}"
    ensure_consumer_group(ORDER_EVENTS, STOCK_ORDER_GROUP)
    app.logger.info(f"Starting order events consumer {consumer_name}")

    while True:
        try:
            messages = db.xreadgroup(
                STOCK_ORDER_GROUP,
                consumer_name,
                {ORDER_EVENTS: '>'},
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

                        app.logger.debug(f"Processing order event: {event_type}")

                        if event_type == ORDER_CREATED:
                            transaction_id = event_data.get('transaction_id')
                            order_id = event_data.get('order_id')
                            items = event_data.get('items', [])
                            total_cost = event_data.get('total_cost')
                            user_id = event_data.get('user_id')
                            response_stream = event_data.get('response_stream')

                            app.logger.info(f"Trying to reserve stock for order {order_id}")

                            reserved_items = []
                            reservation_success = True

                            # Try to reserve
                            for item in items:
                                item_id = item.get('item_id')
                                amount = int(item.get('quantity', 0))
                                try:
                                    reservation_success = make_reservation(item_id, amount)
                                    if reservation_success:
                                        reserved_items.append((item_id, amount))
                                except Exception as e:
                                    app.logger.error(f"Error reserving item {item_id}: {e}")
                                    reservation_success = False
                                    break

                            # If any reservation failed, roll back all successful reservations
                            if not reservation_success:
                                app.logger.warning(f"Rolling back reservations for order {order_id}")
                                for item_id, amount in reserved_items:
                                    try:
                                        release_reservation(item_id, amount)
                                        # Publish failed result to the order-service
                                        db.xadd(response_stream, {
                                            b'result': json.dumps({
                                                "status": "failed",
                                                "reason": "Failed to reserve item",
                                                "order_id": event_data.get('order_id')
                                            }).encode()
                                        })
                                    except Exception as e:
                                        app.logger.error(f"Error rolling back reservation for item {item_id}: {e}")
                            else:
                                # All reservations successful
                                app.logger.info(f"Successfully reserved all items for order {order_id}")

                                # Publish stock reserved event
                                publish_event(STOCK_EVENTS, STOCK_RESERVED, {
                                    "order_id": order_id,
                                    "transaction_id": transaction_id,
                                    "total_cost": total_cost,
                                    "user_id": user_id,
                                    "items": items,
                                    "response_stream": response_stream
                                })
                            app.logger.debug(f"Order {order_id} contains {len(items)} items")

                        # Acknowledge the message
                        db.xack(stream_name, STOCK_ORDER_GROUP, message_id)

                    except Exception as e:
                        app.logger.error(f"Error processing order event {message_id}: {e}")

        except Exception as e:
            app.logger.error(f"Error in order events consumer: {e}")
            time.sleep(1)


def process_payment_events():
    """Process events from the payment stream"""
    consumer_name = f"stock-payment-consumer-{uuid.uuid4()}"
    ensure_consumer_group(PAYMENT_EVENTS, STOCK_PAYMENT_GROUP)
    app.logger.info(f"Starting payment events consumer {consumer_name}")

    while True:
        try:
            messages = db.xreadgroup(
                STOCK_PAYMENT_GROUP,
                consumer_name,
                {PAYMENT_EVENTS: '>'},
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

                        app.logger.debug(f"Processing payment event: {event_type}")

                        if event_type == PAYMENT_FAILED:
                            transaction_id = event_data.get('transaction_id')
                            order_id = event_data.get('order_id')
                            items = event_data.get('items', [])

                            app.logger.info(f"Payment failed for order {order_id}, releasing stock")

                            for item in items:
                                item_id = item.get('item_id')
                                amount = item.get('quantity')

                                if item_id and amount:
                                    success = release_reservation(item_id, amount)
                                    if not success:
                                        app.logger.error(f"Failed to release reservation for item {item_id}")

                        elif event_type == PAYMENT_SUCCEEDED:
                            transaction_id = event_data.get('transaction_id')
                            order_id = event_data.get('order_id')
                            items = event_data.get('items', [])

                            app.logger.info(f"Payment succeeded transaction {transaction_id} for order {order_id} -> confirming stock reduction")

                            for item in items:
                                item_id = item.get('item_id')
                                amount = item.get('quantity')

                                if item_id and amount:
                                    success = confirm_reservation(item_id, amount)
                                    if not success:
                                        app.logger.error(f"Failed to confirm stock reduction for item {item_id}")

                        db.xack(stream_name, STOCK_PAYMENT_GROUP, message_id)

                    except Exception as e:
                        app.logger.error(f"Error processing payment event {message_id}: {e}")

        except Exception as e:
            app.logger.error(f"Error in payment events consumer: {e}")
            time.sleep(1)


def initialize_streams():
    """Initialize Redis Streams and consumer groups"""
    ensure_consumer_group(STOCK_EVENTS, "init-group")
    app.logger.info("Stock events stream initialized")

def start_consumers():
    """Start background threads for event processing"""
    order_consumer_thread = threading.Thread(target=process_order_events, daemon=True)
    payment_consumer_thread = threading.Thread(target=process_payment_events, daemon=True)

    order_consumer_thread.start()
    payment_consumer_thread.start()

    app.logger.info("Event consumers started")


def initialize_app():
    app.logger.info("Initializing stock service event processing")
    initialize_streams()
    start_consumers()

initialize_app()
if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)