# -*- coding:utf-8 -*-
"""
@Time : 2020/4/8 9:17 下午
@Author : Domionlu
@Site : 
@File : mqevent.py
@Software: PyCharm
"""
import zlib
import pika
from config import config
from log import log
import json

class Mqserver:

    def __init__(self):
        self._host =config.rabbitmq.host
        self._port =config.rabbitmq.port
        self._username =config.rabbitmq.username
        self._password =config.rabbitmq.password
        self._protocol = None
        self._connect = None
        self._channel = None  # Connection channel.
        self._connected = False  # If connect success.
        self._subscribers = []  # e.g. [(event, callback, multi), ...]
        self._event_handler = {}  # e.g. {"exchange:routing_key": [callback_function, ...]}
        self.msgcount=0
        self.initialize()

    def initialize(self):
        self.connect()

    def connect(self):
        credentials = pika.PlainCredentials(self._username, self._password)
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=self._host, port=self._port, credentials=credentials))
        channel = connection.channel()
        self._connect=connection
        exchanges = ["Trade", "Ticker", "OrderBook", "Kline.15min", "Asset", "Alert"]
        for name in exchanges:
            channel.exchange_declare(name, "topic");
        self._channel = channel
        log.debug("create default exchanges success!")

    def subscribe(self,exchange,callback,binding_key):
        result = self._channel.queue_declare('', exclusive=True)
        queue_name = result.method.queue
        self._channel.queue_bind(exchange=exchange, queue=queue_name, routing_key=binding_key)
        self._channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)
        self._channel.start_consuming()


    def publish(self,event):
        # log.debug(f"publish exchange:{event.exchange},key:{event.routing_key},data{event.data}")
        self.msgcount+=1
        data = event.dumps()
        self._channel.basic_publish(exchange=event.exchange,
                              routing_key=event.routing_key,
                              body=data)


class Event:
    """ Event base.
    Attributes:
        name: Event name.
        exchange: Exchange name.
        queue: Queue name.
        routing_key: Routing key name.
        pre_fetch_count: How may message per fetched, default is 1.
        data: Message content.
    """
    def __init__(self, name=None, exchange=None, queue=None, routing_key=None, pre_fetch_count=1, data=None):
        """Initialize."""
        self._name = name
        self._exchange = exchange
        self._queue = queue
        self._routing_key = routing_key
        self._pre_fetch_count = pre_fetch_count
        self._data = data
        self._callback = None  # Asynchronous callback function.

    @property
    def name(self):
        return self._name

    @property
    def exchange(self):
        return self._exchange

    @property
    def queue(self):
        return self._queue

    @property
    def routing_key(self):
        return self._routing_key

    @property
    def prefetch_count(self):
        return self._pre_fetch_count

    @property
    def data(self):
        return self._data

    def dumps(self):
        d = {
            "n": self.name,
            "e": self.exchange,
            "k": self.routing_key,
            "d": self.data
        }
        s = json.dumps(d)
        b = zlib.compress(s.encode("utf8"))
        return b

    def loads(self, b):
        b = zlib.decompress(b)
        d = json.loads(b.decode("utf8"))
        self._name = d.get("n")
        self._exchange=d.get("e")
        self._routing_key=d.get("k")
        self._data = d.get("d")
        return d

    def __str__(self):
        info = "EVENT: name={n}, exchange={e}, queue={q}, routing_key={r}, data={d}".format(
            e=self.exchange, q=self.queue, r=self.routing_key, n=self.name, d=self.data)
        return info

    def __repr__(self):
        return str(self)


ev=Even# -*- coding:utf-8 -*-
"""
@Time : 2020/4/8 9:17 下午
@Author : Domionlu
@Site : 
@File : mqevent.py
@Software: PyCharm
"""
import zlib
import pika
from config import config
from log import log
import json

class Mqserver:

    def __init__(self):
        self._host =config.rabbitmq.host
        self._port =config.rabbitmq.port
        self._username =config.rabbitmq.username
        self._password =config.rabbitmq.password
        self._protocol = None
        self._connect = None
        self._channel = None  # Connection channel.
        self._connected = False  # If connect success.
        self._subscribers = []  # e.g. [(event, callback, multi), ...]
        self._event_handler = {}  # e.g. {"exchange:routing_key": [callback_function, ...]}
        self.msgcount=0
        self.initialize()

    def initialize(self):
        self.connect()

    def connect(self):
        credentials = pika.PlainCredentials(self._username, self._password)
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=self._host, port=self._port, credentials=credentials))
        channel = connection.channel()
        self._connect=connection
        exchanges = ["Trade", "Ticker", "OrderBook", "Kline.15min", "Asset", "Alert"]
        for name in exchanges:
            channel.exchange_declare(name, "topic");
        self._channel = channel
        log.debug("create default exchanges success!")

    def subscribe(self,exchange,callback,binding_key):
        result = self._channel.queue_declare('', exclusive=True)
        queue_name = result.method.queue
        self._channel.queue_bind(exchange=exchange, queue=queue_name, routing_key=binding_key)
        self._channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)
        self._channel.start_consuming()


    def publish(self,event):
        # log.debug(f"publish exchange:{event.exchange},key:{event.routing_key},data{event.data}")
        self.msgcount+=1
        data = event.dumps()
        self._channel.basic_publish(exchange=event.exchange,
                              routing_key=event.routing_key,
                              body=data)


class Event:
    """ Event base.
    Attributes:
        name: Event name.
        exchange: Exchange name.
        queue: Queue name.
        routing_key: Routing key name.
        pre_fetch_count: How may message per fetched, default is 1.
        data: Message content.
    """
    def __init__(self, name=None, exchange=None, queue=None, routing_key=None, pre_fetch_count=1, data=None):
        """Initialize."""
        self._name = name
        self._exchange = exchange
        self._queue = queue
        self._routing_key = routing_key
        self._pre_fetch_count = pre_fetch_count
        self._data = data
        self._callback = None  # Asynchronous callback function.

    @property
    def name(self):
        return self._name

    @property
    def exchange(self):
        return self._exchange

    @property
    def queue(self):
        return self._queue

    @property
    def routing_key(self):
        return self._routing_key

    @property
    def prefetch_count(self):
        return self._pre_fetch_count

    @property
    def data(self):
        return self._data

    def dumps(self):
        d = {
            "n": self.name,
            "e": self.exchange,
            "k": self.routing_key,
            "d": self.data
        }
        s = json.dumps(d)
        b = zlib.compress(s.encode("utf8"))
        return b

    def loads(self, b):
        b = zlib.decompress(b)
        d = json.loads(b.decode("utf8"))
        self._name = d.get("n")
        self._exchange=d.get("e")
        self._routing_key=d.get("k")
        self._data = d.get("d")
        return d

    def __str__(self):
        info = "EVENT: name={n}, exchange={e}, queue={q}, routing_key={r}, data={d}".format(
            e=self.exchange, q=self.queue, r=self.routing_key, n=self.name, d=self.data)
        return info

    def __repr__(self):
        return str(self)




if __name__ == "__main__":
    pass


if __name__ == "__main__":
    pass
