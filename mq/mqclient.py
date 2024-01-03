import functools
import logging
import json
import os
import pika
import logging
import time
from enum import Enum
from dotenv import load_dotenv
from pika.exchange_type import ExchangeType
from pika.adapters.asyncio_connection import AsyncioConnection
from pika.channel import Channel
import uuid
load_dotenv()

class ProducerStatus(Enum):
    Shutdown = 1
    Running = 2
    

class TaskProducer:   
    status : ProducerStatus = ProducerStatus.Shutdown
    user_id : str
    amqp_url: str
    exchange : str
    request_queue_name: str
    response_queue_name : str
    request_routing_key : str
    response_routing_key : str
    connection : pika.BlockingConnection or AsyncioConnection
    request_channel: Channel
    response_channel : Channel
    logger : logging.Logger
    message_handler : callable(dict) or None

    def __init__(self) -> None:
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)
        stream_handler = logging.StreamHandler()
        formatter = logging.Formatter("%(asctime)s %(levelname)s: %(message)s", datefmt='%Y-%m-%d %H:%M:%S')
        stream_handler.setFormatter(formatter)
        self.logger.addHandler(stream_handler)
        self.amqp_url = os.environ.get("AMQP_URL")
        self.user_id = str(uuid.uuid4())
        self.task_id = str | None
        self.consumer_tag = str
        self.exchange = os.environ.get("REQUEST_EXCHANGE_NAME")
        self.request_queue_name = os.environ.get("REQUEST_QUEUE")
        self.request_routing_key = os.environ.get("REQUEST_ROUTING_KEY")
        self.response_queue_name = os.environ.get("RESPONSE_QUEUE")
        self.response_routing_key = os.environ.get("RESPONSE_ROUTING_KEY")
        
    def open(self):
        self.connection = AsyncioConnection(pika.URLParameters(self.amqp_url))
        self.logger.info(f"Producer #{self.user_id} connected to {self.amqp_url}")
        # setup request channel
        self.request_channel = self.connection.channel()
        self.request_channel.exchange_declare(exchange=self.exchange, exchange_type=ExchangeType.topic)
        self.request_channel.queue_declare(self.request_queue_name, durable=True)
        self.request_channel.queue_bind(self.request_queue_name, self.exchange, routing_key=self.request_routing_key)
        self.logger.info(f"Producer #{self.user_id} on {self.exchange} with request channel : {self.request_queue_name} #{self.request_routing_key}")
        # setup response channel
        self.response_channel = self.connection.channel()
        self.response_channel.exchange_declare(exchange=self.exchange, exchange_type=ExchangeType.topic)
        self.response_channel.queue_declare(self.response_queue_name, durable=True)
        self.response_channel.queue_bind(self.response_queue_name, self.exchange, routing_key=self.response_routing_key)
        self.logger.info(f"Producer #{self.user_id} on {self.exchange} with response channel : {self.response_queue_name} #{self.response_routing_key}") 
        self.status = ProducerStatus.Running
        
    def close(self):
        self.stop_consuming()
        self.connection.close()
        self.status = ProducerStatus.Shutdown
        self.logger.info("Producer shutdown")
        
    def send(self, data : dict):
        self.task_id = str(uuid.uuid4())
        self.request_channel.basic_publish(
            exchange=self.exchange,
            routing_key=self.request_routing_key,
            body=json.dumps(data, ensure_ascii=False),
            properties=pika.BasicProperties(content_type="application/json", correlation_id=self.task_id,
                                            reply_to=self.response_routing_key, timestamp=int(time.time()))
        )
        self.logger.info(f"Request #{self.task_id} sent")
    
    def start_consuming(self):
        self.consumer_tag = self.response_channel.basic_consume(self.response_queue_name, on_message_callback=self.on_message, auto_ack=False)
        self.message_handler = self.handler
        self.logger.info(f"Consumer #{self.consumer_tag} start consuming on {self.response_queue_name}")
        
    def stop_consuming(self):
        self.response_channel.basic_cancel(self.consumer_tag)
        self.logger.info(f"Consumer #{self.consumer_tag} stop consuming on {self.response_queue_name}")



        
    def on_message(self, channel, deliver, basic_properties, body):
        if not self.task_id or not basic_properties.correlation_id == self.task_id:
            # do not ack message if is not for us
            return
        else:
            if basic_properties.content_type == "application/json":
                data = json.loads(body.decode('utf8'))
                if self.message_handler is not None:
                    self.message_handler(data)
                    use_time = int(time.time()) - basic_properties.timestamp 
                    self.logger.info(f"Recived response from request #{basic_properties.correlation_id} use {use_time} seconds")
                    self.response_channel.basic_ack(deliver.delivery_tag)
    
    def handler(self, data : dict):
        self.logger.info(f"Recive data : {data}")
