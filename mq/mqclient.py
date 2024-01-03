import functools
import logging
import json
import os
import pika
import logging
from dotenv import load_dotenv
from pika.exchange_type import ExchangeType
from pika.channel import Channel
load_dotenv()

class TaskProducer:
    amqp_url: str
    exchange : str
    request_queue_name: str
    request_routing_key : str
    connection : pika.BlockingConnection
    channel: Channel
    logger : logging.Logger

    def __init__(self) -> None:
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)
        stream_handler = logging.StreamHandler()
        formatter = logging.Formatter("%(asctime)s %(levelname)s: %(message)s", datefmt='%Y-%m-%d %H:%M:%S')
        stream_handler.setFormatter(formatter)
        self.logger.addHandler(stream_handler)
        self.amqp_url = os.environ.get("AMQP_URL")
        self.connection = pika.BlockingConnection(pika.URLParameters(self.amqp_url))
        self.channel = self.connection.channel()
        self.logger.info(f"Producer connected to {self.amqp_url}")
        self.exchange = os.environ.get("REQUEST_EXCHANGE_NAME")
        self.request_routing_key = os.environ.get("REQUEST_ROUTING_KEY")
        self.request_queue_name = os.environ.get("REQUEST_QUEUE")
        self.channel.exchange_declare(exchange=self.exchange, exchange_type=ExchangeType.topic)
        self.channel.queue_declare(self.request_queue_name, durable=True)
        self.channel.queue_bind(self.request_queue_name, self.exchange, routing_key=self.request_routing_key)
        self.logger.info(f"Producer on {self.exchange} with {self.request_queue_name} #{self.request_routing_key}")
        
    def send(self, data : dict):
        self.channel.basic_publish(
            exchange=self.exchange,
            routing_key=self.request_routing_key,
            body=json.dumps(data, ensure_ascii=False),
            properties=pika.BasicProperties(content_type="application/json")
        )