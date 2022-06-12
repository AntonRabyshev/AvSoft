import json
import os
from uuid import uuid4
import time

import pika
from mysql.connector import connect, Error
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from dotenv import load_dotenv

from mylogger import logger
from parser import Database

load_dotenv()
input_dir = os.getenv('input_dir')


class MyHandler(FileSystemEventHandler):
    """
    Class MyHandler, Parent class: FileSystemEventHandler.
    Waiting for a file to appear in the folder and handles this event
    """

    def on_created(self, event):
        try:
            if os.path.isfile(event.src_path):
                with Database() as db:
                    get_path_from_table = "SELECT * from path_to_file WHERE path_as_str = %s"
                    db.execute(get_path_from_table, [event.src_path])
                    data = db.cursor.fetchone()
                    if not data:
                        file_id = str(uuid4())
                        message = {file_id: event.src_path}
                        insert_query = "INSERT INTO path_to_file (path_as_str) values (%s)"
                        db.execute(insert_query, [event.src_path])
                        db.commit()
        except Exception as e:
            logger.error(f'an exception occurred while trying to interact with db: {e}')
        try:
            if event.src_path.endswith('.txt'):
                sending_msg_to_queue('Parsing', message)
            else:
                sending_msg_to_queue('Errors', message)
        except Exception as e:
            logger.error(f'an exception occurred while trying to send a message with: {e}')


def sending_msg_to_queue(q_name: str, msg: dict) -> None:
    """
    Establishing connection with RabbitMQ and sending message with path to file for any queue
    :param q_name: name of queue
    :param msg: message being sent
    """
    prop = pika.BasicProperties(delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE)
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    channel.queue_declare(queue=q_name, durable=True)
    channel.basic_publish(exchange='', routing_key=q_name, properties=prop, body=json.dumps(msg))
    connection.close()


if __name__ == '__main__':
    try:
        with connect(
                host=os.getenv('host'),
                user=os.getenv('user'),
                password=os.getenv('password'),
        ) as connection:
            create_db_query = 'CREATE DATABASE IF NOT EXISTS Parsing'
            with connection.cursor() as cursor:
                cursor.execute(create_db_query)
    except Error as e:
        logger.error(f'an exception occurred while trying to connect to db: {e}')
    try:
        with Database() as db:
            create_path_table = """
            CREATE TABLE IF NOT EXISTS path_to_file(
                id INT AUTO_INCREMENT PRIMARY KEY,
                path_as_str VARCHAR(500)
            )
            """
            db.execute(create_path_table)
            db.commit()
    except Error as e:
        logger.error(f'an exception occurred while trying to create a table: {e}')
    try:
        event_handler = MyHandler()
        observer = Observer()
        observer.schedule(event_handler, input_dir, recursive=True)
        observer.start()
    except Exception as e:
        logger.error(f'an exception occurred while trying to run observer: {e}')
    try:
        while True:
            time.sleep(0.1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()
