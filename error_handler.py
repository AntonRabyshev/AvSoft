import json
import os
import sys

import pika
from dotenv import load_dotenv
import telebot

from mylogger import logger
from utils import retry_with_params

load_dotenv()
bot = telebot.TeleBot(os.getenv('telebot_token'))


@retry_with_params
def sending_telegram_message(msg_w_path: str) -> None:
    """
    Sending message from telegram bot to user with path to file from Errors queue
    :param msg_w_path: path to file from Errors queue
    """
    try:
        text = ('Message from Errors queue: ', msg_w_path)
        msg = ''.join(text)
        bot.send_message(os.getenv('user_id'), msg)
    except Exception as e:
        logger.error(f'an exception occurred while trying to send a message via telegram: {e}')


def receiving_message() -> None:
    """
    Establishing connection with RabbitMQ and receiving message with path to file from "Errors" queue
    """
    connection_rmq = pika.BlockingConnection(pika.ConnectionParameters(host='gateway.docker.internal'))
    channel = connection_rmq.channel()
    channel.queue_declare(queue='Errors', durable=True)

    def callback(ch, method, properties, body) -> None:
        logger.debug(f" [x] Received {body}")
        path_as_dict = json.loads(body)
        path_as_str = list(path_as_dict.values())[0]
        sending_telegram_message(path_as_str)
        ch.basic_ack(delivery_tag=method.delivery_tag)

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue='Errors', on_message_callback=callback)
    channel.start_consuming()


if __name__ == '__main__':
    logger.debug(f'error_handler has started')
    try:
        receiving_message()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
