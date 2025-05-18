import json
from typing import Dict
import pika
from decouple import config

class RabbitmqPublisher:
    def __init__(self):
        self.__host = config("RABBITMQ_HOST")
        self.__port = config("RABBITMQ_PORT")
        self.__username = config("RABBITMQ_USERNAME")
        self.__password = config("RABBITMQ_PASSWORD")
        self.__exchange = "data_exchange"
        self.__routing_key = ""
        self.__channel = self.__create_chanel()
    
    def __create_chanel(self):
        connection_parameters = pika.ConnectionParameters(
            host= self.__host,
            port= self.__port,
            credentials=pika.PlainCredentials(
                username=self.__username, password=self.__password
            )
        )

        channel = pika.BlockingConnection(connection_parameters).channel()
        
        return channel

    def send_message(self, body: Dict) -> None:
        self.__channel.basic_publish(
            exchange=self.__exchange,
            routing_key=self.__routing_key,
            body=json.dumps(body),
            properties=pika.BasicProperties(
                delivery_mode=2
            )
        )

rabbitmq_publish = RabbitmqPublisher()
rabbitmq_publish.send_message({"ola":"mundo"})
