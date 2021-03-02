import logging
import os
import time
import datetime
from multiprocessing.pool import Pool

import requests
import sentry_sdk
from bs4 import BeautifulSoup
from rq import Queue

from utils.logger import custom_logger
from connector.connector import RedisConnector, MongoDBConnector
from danawa.crawler import DanawaSearcher

custom_logger = custom_logger()
logger = custom_logger.getLogger(__name__)

sentry_sdk.init("")

# Worker Queue
workerQueue = Queue('high', connection=RedisConnector().conn(), default_timeout=1800)

# Comment Queue
commentQueue = Queue('low', connection=RedisConnector().conn(), default_timeout=1800)


def item_parse(item: BeautifulSoup) -> list:
    # 상품명 파싱: 이 값은 크롤링에 사용하지 않고, 로깅에 출력을 위해 파싱한다
    # 상품명은 URL 로 상품 상세 정보에서 파싱할 때 별도로 파싱한다
    try:
        item_name = item.find('div', {'class': 'prod_main_info'})\
            .find('div', {'class': 'prod_info'})\
            .find('p', {'class': 'prod_name'})\
            .find('a')\
            .get_text(strip=True)
    except AttributeError:
        item_name = None

    # 제품별 가격이 여러개 나와 있는 경우에 item_name 추가 및 개별 링크 설정
    price_list = []
    try:
        item_price_list = item.find('div', {'class': 'prod_main_info'})\
            .find('div', {'class': 'prod_pricelist'})\
            .find('ul')\
            .find_all('li')
    except AttributeError:
        price_list = []
    else:
        for item_price in item_price_list:
            # x 위 로 표시되어 있는 span Tag 를 삭제한다
            try:
                item_price.find('span', {'class': 'rank'}).decompose()
            except AttributeError:
                pass

            # 제품 구분이나 단위가 아닌 ml당 가격 등 불필요 텍스트는 삭제한다
            try:
                item_price.find('em', {'class': 'lowest'}).decompose()
            except AttributeError:
                pass
            try:
                item_price.find('span', {'class': 'memory_price_sect'}).decompose()
            except AttributeError:
                pass

            # 개별 상품 추가 이름 파싱(eg. '기획세트', '단품세트', '1개' 등)
            try:
                parted_item_price_name = item_price.find('p', {'class': 'memory_sect'}).get_text(strip=True)
            except AttributeError:
                parted_item_price_name = None

            try:
                item_link = item_price.find('p', {'class': 'memory_sect'}).find('a')['href']
            except AttributeError:
                item_link = None

            obj = {
                'item': parted_item_price_name,
                'url': item_link
            }
            price_list.append(obj)

    # 가격별 상품명과 원래 상품명을 합친다
    ret = []
    for i in price_list:
        if i['item']:
            name = f'{item_name}({i["item"]})'
        else:
            name = item_name
        ret.append({
            'item': name,
            'url': i['url']
        })

    return ret


def item_scrape(item_list: [BeautifulSoup], keyword: str) -> None:
    """
    매개변수로 받은 상품 목록을 파싱하여 worker Queue 에 넣는다
    """
    for item in item_list:
        parsed_item = item_parse(item)
        # parse item 은 list 형태로 반환되므로 개별로 Queue 에 넣는다
        for pitem in parsed_item:
            logging.info(f'{pitem["item"]}'
                         f' | {pitem["url"]}')

            # item 상품명과 url 이 모두 있는 아이템의 경우에만 Queue 에 넣는다
            if pitem['item'] and pitem['url']:
                workerQueue.enqueue('worker.product_parser', pitem['url'], keyword, job_timeout=43200, result_ttl=86400)


def main(keyword: str):
    page = 1
    searcher = DanawaSearcher()

    while page <= searcher.limit:
        try:
            data = searcher.fetch(keyword, page)
        except Exception as e:
            raise requests.RequestException(f'init search page requests error: {repr(e)}')
        else:
            data = BeautifulSoup(data, 'lxml')

        logging.info(f'Current page: {page} of {searcher.limit} | {keyword}')

        # 페이지 limit 이 1이라면 검색 결과에서 item 개수로 page 수를 계산한다
        if searcher.limit == 1:
            try:
                item_count = data.find('div', {'class': 'category_selector'})\
                    .find('div', {'class': 'tab_header'})\
                    .find('ul', {'class': 'goods_type'})\
                    .find('a', {'class': 'vmTab'})['data-count']
            except:
                logging.error(f'search item count parsed fail(keyword: {keyword})')
                break
            else:
                item_count = int(item_count)

                # item 개수로 page 수를 계산하여 limit 를 설정한다
                if item_count % 90 == 0:
                    searcher.limit = int(item_count // 90)
                else:
                    searcher.limit = int(item_count // 90) + 1

        item_list = data.find('div', {'class': 'main_prodlist main_prodlist_list'})\
            .find('ul', {'class': 'product_list'})\
            .find_all('li', {'class': 'prod_item'})
        if item_list:
            item_scrape(item_list, keyword)
        else:
            logging.info(f'page inner item is none: {page}')
            break

        page += 1
        time.sleep(60)


if __name__ == '__main__':
    logging.info(f'Start crawling....{datetime.datetime.now()}')
    start = time.time()

    proc_count = os.cpu_count()
    pool = Pool(processes=proc_count)
    pool.map(main, MongoDBConnector().random_keyword())

    end = time.time()
    logging.info(f'End crawling... time taken: {end - start}')
