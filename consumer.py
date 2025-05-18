import pika
from decouple import config

class RabbitmaConsumer:
    def __init__(self, callback) -> None:
        self.__host = config("RABBITMQ_HOST")
        self.__port = config("RABBITMQ_PORT")
        self.__username = config("RABBITMQ_USERNAME")
        self.__password = config("RABBITMQ_PASSWORD")
        self.__gueue = "data_queue"
        self.__callback = callback
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
        channel.queue_declare(
            queue=self.__gueue,
            durable=True
        )

        channel.basic_consume(
            queue=self.__gueue,
            auto_ack=True,
            on_message_callback=self.__callback
        )

        return channel

    def start(self):
        print(f"Listen RabbitMQ on Port {self.__port}")
        self.__channel.start_consuming()


def my_callback(ch, method, properties, body):
    print(body)

rabbitma_consumer = RabbitmaConsumer(my_callback)
rabbitma_consumer.start()

