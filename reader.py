from datetime import datetime
import os

from dotenv import load_dotenv

from mylogger import logger
from parser import Database

n = 10
load_dotenv()
path = os.path.join(os.getenv('create_folder_dir'), 'reader_result')


def write_words_into_file(path_to_folder: str) -> None:
    """
    Writing words and file they are in into .txt file from mySQL table
    :param path_to_folder: path to folder, where new files must be created
    """
    try:
        with Database() as db:
            get_word_from_table = "SELECT * from words_in_text WHERE num_of_repeats >= %s"
            db.execute(get_word_from_table, [n])
            data = db.cursor.fetchall()
            for i_row in data:
                file_name = i_row[1] + str(datetime.now().strftime('%m_%d_%Y_%H_%M_%S')) + '.txt'
                file_name_w_path = os.path.join(path_to_folder, file_name)
                try:
                    with open(file_name_w_path, 'w', encoding='utf-8') as file:
                        file.write(i_row[1] + '\n')
                        file.write(i_row[3])
                except Exception as e:
                    logger.error(f'an exception occurred while trying to create a file or write to a file: {e}')
                else:
                    remove_from_table = "DELETE from words_in_text WHERE word = %s"
                    db.execute(remove_from_table, [i_row[1]])
                    db.commit()
    except Exception as e:
        logger.error(f'an exception occurred while trying to interact with db: {e}')


if __name__ == '__main__':
    logger.debug(f'reader has started')
    try:
        if not os.path.exists(path):
            os.mkdir(path)
    except Exception as e:
        print(e)
    write_words_into_file(path)
