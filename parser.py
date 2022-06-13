import json
import os
from typing import Dict

import pika
import re
import sys
from dotenv import load_dotenv
from mysql.connector import connect, Error

from mylogger import logger

load_dotenv()


class Database:
    """
    Class Database to work with mySQL database
    """

    def __init__(self):
        self.__conn = connect(
            host=os.getenv('host'),
            user=os.getenv('user'),
            password=os.getenv('password'),
            database='Parsing',
            port=3308
        )
        self.__cursor = self.__conn.cursor()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.__conn.close()

    @property
    def connection(self):
        return self.__conn

    @property
    def cursor(self):
        return self.__cursor

    def execute(self, sql, params=None):
        self.cursor.execute(sql, params or ())

    def commit(self):
        self.connection.commit()

    def close(self, commit=True):
        if commit:
            self.commit()
        self.connection.close()

    def fetchall(self):
        return self.cursor.fetchall()


def find_whole_word(w: str):
    """
    Checks if the searched word is in the string
    :param w: word to search in string
    :return: re.Compile type object
    """
    return re.compile(r'\b({0})\b'.format(w), flags=re.IGNORECASE).search


def split_words(file_path: str) -> Dict[str, int]:
    """
    Splitting words from file and adding them and their quantity in dict
    :param file_path: path to file from Parser queue
    :return: words_in_file
    """
    words_in_file: Dict[str, int] = dict()
    with open(file_path, 'r', encoding='utf-8') as file:
        for i_string in file:
            i_string_w_spaces = re.sub("[^a-zA-Zа-яА-Я]+", " ", i_string).strip()
            i_string_list = re.split(" ", i_string_w_spaces)
            for i_word in i_string_list:
                if i_word not in words_in_file.keys():
                    words_in_file[i_word] = 1
                else:
                    words_in_file[i_word] += i_string_list.count(i_word)
    return words_in_file


def write_words_into_table(words_dict: dict, file_name: str) -> None:
    """
    Writing a dict of words and the filename they are in to a mySQL table
    :param words_dict: dict of words and their quantity
    :param file_name: name of file with words to pass into table
    """
    try:
        with Database() as db:
            for i_word, i_count in words_dict.items():
                get_word_from_table = "SELECT * from words_in_text WHERE word = %s"
                db.execute(get_word_from_table, [i_word])
                data = db.cursor.fetchone()
                if data:
                    if not find_whole_word(file_name)(data[3]):
                        all_file_names = ', '.join((data[3], file_name))

                        update_query = "UPDATE words_in_text " \
                                       "SET num_of_repeats = num_of_repeats + %s, name_of_file = %s WHERE word=%s"
                        db.execute(update_query, [i_count, all_file_names, i_word])
                        db.commit()
                    else:
                        update_query = "UPDATE words_in_text " \
                                       "SET num_of_repeats = num_of_repeats + %s WHERE word=%s"
                        db.execute(update_query, [i_count, i_word])
                        db.commit()
                else:
                    insert_query = "INSERT INTO words_in_text (word, num_of_repeats, name_of_file) values (%s, %s, %s)"
                    db.execute(insert_query, [i_word, i_count, file_name])
                    db.commit()
    except Error as e:
        logger.error(f'an exception occurred while trying to interact with db: {e}')


def receiving_message() -> None:
    """
    Establishing connection with RabbitMQ and receiving message with path to file from "Parsing" queue
    """
    connection_rmq = pika.BlockingConnection(pika.ConnectionParameters(host='gateway.docker.internal'))
    channel = connection_rmq.channel()
    channel.queue_declare(queue='Parsing', durable=True)

    def callback(ch, method, properties, body) -> None:
        logger.debug(f" [x] Received {body}")
        path_as_dict = json.loads(body)
        path_as_str = list(path_as_dict.values())[0]
        name_of_file = os.path.basename(path_as_str)
        dict_of_words = split_words(path_as_str)
        write_words_into_table(dict_of_words, name_of_file)
        ch.basic_ack(delivery_tag=method.delivery_tag)

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue='Parsing', on_message_callback=callback)
    channel.start_consuming()


if __name__ == '__main__':
    logger.debug(f'parser has started')
    try:
        # создаем таблицу
        with Database() as db:
            create_parsing_table = """
            CREATE TABLE IF NOT EXISTS words_in_text(
                id INT AUTO_INCREMENT PRIMARY KEY,
                word VARCHAR(100),
                num_of_repeats INT,
                name_of_file VARCHAR(16000)
            )
            """
            db.execute(create_parsing_table)
            db.commit()
    except Error as e:
        logger.error(f'an exception occurred while trying to create a table: {e}')

    try:
        receiving_message()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
