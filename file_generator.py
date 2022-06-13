import os
from datetime import datetime
from typing import Generator

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

from mylogger import logger

load_dotenv()
input_dir = os.getenv('input_dir')


def file_gen(url_address: str) -> Generator[str, None, None]:
    """
    Generates urls of pages from root url
    :param url_address: root url
    """
    try:
        reqs = requests.get(url_address)
        soup = BeautifulSoup(reqs.text, 'html.parser')
        for link in soup.find_all('a'):
            if link.get('href').startswith('https'):
                yield link.get('href')
    except Exception as e:
        logger.error(f'an exception occurred while trying to get urls: {e}')


if __name__ == '__main__':
    logger.debug(f'file gen has started')
    url = os.getenv('sitemap_url')
    if url.startswith('www'):
        url = 'http://' + url
    elif not url.startswith('http'):
        url = 'https://www.' + url
    response = requests.get(url)
    if response.status_code == 200:
        urls = file_gen(url)
        try:
            for url in urls:
                url_request = requests.get(url).text
                soup = BeautifulSoup(url_request, 'html.parser')
                if soup.find_all('p'):
                    file_name = str(datetime.now().strftime('%m_%d_%Y_%H_%M_%S')) + '.txt'
                    file_name_w_path = os.path.join(input_dir, file_name)
                    with open(file_name_w_path, 'w', encoding='utf-8') as file:
                        for article_body in soup.find_all('p'):
                            body = article_body.text
                            file.write(body)
        except Exception as e:
            logger.error(f'an exception occurred while trying to get content from urls: {e}')
    else:
        print('Web site does not exist')
