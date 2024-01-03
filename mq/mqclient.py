import functools
import logging
import json
import os
import pika
from dotenv import load_dotenv
from pika.exchange_type import ExchangeType
from pika.channel import Channel
load_dotenv()

class TaskProducer:
    amqp_url: str
    exchange : str
    connection : pika.BlockingConnection
    channel: Channel

    def __init__(self) -> None:
        self.amqp_url = os.environ.get("AMQP_URL")
        self.connection = pika.BlockingConnection(pika.URLParameters(self.amqp_url))
        self.channel = self.connection.channel()
        self.exchange = os.environ.get("REQUEST_EXCHANGE_NAME")
        self.channel.exchange_declare(exchange=self.exchange, exchange_type=ExchangeType.topic)