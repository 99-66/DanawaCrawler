import re
import time
from random import uniform
from datetime import datetime

from bs4 import BeautifulSoup
from rq import Queue

from danawa.crawler import DanawaCrawler
from connector.connector import MongoDBConnector
from utils.hash import generator_chash


# comment Queue
commentQueue = Queue('low')


def product_parser(url, keyword):
    """
    개별 상품 페이지를 파싱하여 저장한다
    """
    # 상품 페이지 파싱 전 딜레이를 준다
    time.sleep(uniform(45, 60))

    dc = DanawaCrawler()

    # 빈 링크인 경우 함수를 종료한다
    if url.startswith('#'):
        return

    main_html = dc.read(url)
    # 상품 상세 주소가 다른 사이트로 연결 시킨다면 함수를 중지한다
    if main_html.url.startswith('http://prod.danawa.com/bridge/'):
        return
    else:
        main_html_bs = BeautifulSoup(main_html.content, 'lxml')

    # 크롤링 시작 시간
    crawlAtTimestamp = int(time.mktime((datetime.now()).timetuple()))

    # 상품 이름 파싱
    try:
        product_name = main_html_bs.find('div', {'class': 'top_summary'}).find('h3').get_text()
    except AttributeError:
        product_name = None

    # 상품 등록월 파싱
    try:
        product_registration_month = main_html_bs.find('div', {'class': 'summary_info'})\
            .find('div', {'class': 'detail_summary'})\
            .find('div', {'class': 'thumb_area'})\
            .find('div', {'class': 'made_info'})\
            .find('span', {'class': 'txt'})\
            .get_text()
    except AttributeError:
        product_registration = None
    else:
        try:
            product_registration_month = product_registration_month.split(':')[1].strip()
        except IndexError:
            product_registration = None
        else:
            product_registration = int(time.mktime(datetime.strptime(product_registration_month, '%Y.%m').timetuple()))

    # 상품 제조사 파싱
    try:
        product_maker = main_html_bs.find('div', {'class': 'summary_info'})\
            .find('div', {'class': 'detail_summary'})\
            .find('div', {'class': 'thumb_area'})\
            .find('span', {'id': 'makerTxtArea'})\
            .get_text(strip=True)
    except AttributeError:
        product_maker = None
    else:
        try:
            product_maker = product_maker.split(':')[1].strip()
        except:
            product_maker = None

    # 상품 가격비교 요청과 상품 상세정보를 가져오기 위한 Script 내의 변수 파싱
    try:
        scripted_data = main_html_bs.find_all('script', {'src': False})
    except AttributeError:
        raise AttributeError('Script parse failed...stopped parse product')
    else:
        unscripted_data = dc.parsed_html_script(scripted_data)

    # 상품 페이지의 가격순위 Summary 파싱
    lowest_list = []
    try:
        price_lowest_summary = main_html_bs.find('div', {'class': 'lowest_area'}) \
            .find('div', {'class': 'lowest_list'}) \
            .find('table', {'class': 'lwst_tbl'}) \
            .find('tbody', {'class': 'high_list'}) \
            .find_all('tr')
    except AttributeError:
        # 가격정보가 없는 상품의 경우 빈 값으로 채우고, 필드는 유지한다
        lowest_list.append({
            'mall': None,
            'price': None,
            'shipping': None,
            'benefit': None,
            'option': None,
            'summary_rank': None}
        )
    else:
        for summary_rank, lowest in enumerate(price_lowest_summary, 1):
            if lowest.get('class'):
                option = lowest.get('class')[0].strip()
            else:
                option = None

            try:
                mall = lowest.find('td', {'class': 'mall'}).find('img')['alt'].strip()
            except AttributeError:
                try:
                    mall = lowest.find('td', {'class': 'mall'})\
                        .find('div', {'class': 'logo_over'})\
                        .find('a').get_text(strip=True)
                except AttributeError:
                    mall = None
            except TypeError:
                try:
                    mall = lowest.find('td', {'class': 'mall'}) \
                        .find('div', {'class': 'logo_over'}) \
                        .find('a').get_text(strip=True)
                except AttributeError:
                    mall = None

            try:
                price = lowest.find('td', {'class': 'price'}).find('span', {'class': 'txt_prc'}).find('em').get_text().strip()
            except AttributeError:
                price = None
            else:
                price = price.replace(',', '').replace('원', '')

            try:
                shipping = lowest.find('td', {'class': 'ship'}).find('span', {'class': 'stxt'}).get_text().strip()
            except AttributeError:
                shipping = None
            else:
                shipping = shipping.replace(',', '').replace('원', '')

            if lowest.find('td', {'class': 'bnfit'}).find('a'):
                benefit = lowest.find('td', {'class': 'bnfit'}).find('a').get_text().strip()
            else:
                benefit = None

            obj = {
                'mall': mall,
                'price': price,
                'shipping': shipping,
                'benefit': benefit,
                'option': option,
                'summary_rank': summary_rank
            }
            lowest_list.append(obj)

    price_summary = lowest_list

    # foreign key 용도의 id 생성
    fid = f'{dc.pcode}_{dc.cate}'
    # product_uid 는 저장되는 상품의 유니크 값이다
    product_uid = f'{fid}_{keyword}'

    # dc.category['GroupName'] 카테고리 정보가 javascript 에 없는 경우 처리
    # 이 값이 없다는 것은, 페이지 javascript 에 카테고리 정보가 없다는 것이므로,
    # 다른 카테고리 값을 사용한다
    if 'GroupName' in dc.category:
        try:
            sectionCategory = dc.category['GroupName']
            category = dc.category['1']['name']
            subCategory = dc.category['2']['name']
        except KeyError:
            physical_cate_list = unscripted_data['physical_category']
            sectionCategory = physical_cate_list[0]
            category = physical_cate_list[1]
            subCategory = physical_cate_list[2]
    else:
        physical_cate_list = unscripted_data['physical_category']
        sectionCategory = physical_cate_list[0]
        category = physical_cate_list[1]
        subCategory = physical_cate_list[2]

    # 상품 정보 리턴
    product_info = {
        '_id': product_uid,
        'fkey': fid,
        'query': keyword,
        'brands': product_maker,
        'productName': product_name,
        'priceSummary': price_summary,
        'publishedAtTimestamp': product_registration,
        'crawlAtTimestamp': crawlAtTimestamp,
        'sectionCategory': sectionCategory,
        'category': category,
        'subCategory': subCategory
    }

    conn = MongoDBConnector().conn()
    collection = conn['']['']

    # 상품 정보 - Upsert MongoDB
    collection.replace_one({'_id': product_info['_id']}, product_info, upsert=True)

    # 코맨트 파싱은 별도로 처리하기 위해 큐에 삽입한다
    commentQueue.enqueue('worker.comment_scrape_and_save', dc, fid, job_timeout=43200, result_ttl=86400)


def comment_save(danawa_comment_list, fkey, collection):
    """
    댓글을 MongoDB 에 있는지 검사한 후 insert 한다
    """
    for danawa_comment in danawa_comment_list:
        # 댓글의 comment hash 값을 생성한다
        chash = generator_chash(danawa_comment, fkey)

        # 댓글이 존재하지 않는 경우 insert 한다
        if not collection.find_one({'_id': chash}):
            danawa_comment['_id'] = chash
            danawa_comment['fkey'] = fkey
            danawa_comment['crawlAtTimestamp'] = int(time.mktime((datetime.now()).timetuple()))

            collection.insert_one(danawa_comment)


def comment_scrape_and_save(dc, fid):
    """
    상품의 다나와 리뷰와 쇼핑몰 리뷰를 크롤링하여 저장한다
    """
    conn = MongoDBConnector().conn()
    collection = conn['']['']

    # 상품 리뷰: 다나와 리뷰 파싱
    danawa_review_html = dc.read_danawa_review()
    danawa_review_bs = BeautifulSoup(danawa_review_html.content, 'lxml')

    # 다나와 리뷰와 쇼핑몰 리뷰 카운팅
    danawa_review_count = 0
    mall_review_count = 0

    review_tab_check = danawa_review_bs.find('div', {'class': 'sub_tab sub_tab_v2'}).find_all('li', {'class': 'tab_item'})
    if review_tab_check:
        for review in review_tab_check:
            danawa_tab = review.find('a', {'id': 'danawa-prodBlog-productOpinion-button-tab-productOpinion'})
            mall_tab = review.find('a', {'id': 'danawa-prodBlog-productOpinion-button-tab-companyReview'})
            if danawa_tab:
                try:
                    danawa_review_count = danawa_tab.find('span', {'class': 'cen_w'}).strong.get_text()
                except AttributeError:
                    continue
                else:
                    danawa_review_count = danawa_review_count.replace(',', '')

            if mall_tab:
                try:
                    mall_review_count = mall_tab.find('span', {'class': 'cen_w'}).strong.get_text()
                except AttributeError:
                    continue
                else:
                    mall_review_count = mall_review_count.replace(',', '')

    # 이 상품에 대한 저장된 댓글의 개수를 가져온다
    collection_danawa_comment_count = collection.find({'fkey': fid, 'source': 'danawa'}).count()

    # 댓글 수가 저장된 개수와 차이가 없거나, 저장된 댓글이 더 많은 경우 추가로 요청하지 않는다
    if collection_danawa_comment_count >= int(danawa_review_count):
        pass
    else:
        diff_danawa_comment_count = int(danawa_review_count) - int(collection_danawa_comment_count)
        # 댓글 크롤링을 위한 페이지 수 설정
        if int(diff_danawa_comment_count) % 10 != 0:
            comment_page_count = int(int(diff_danawa_comment_count)/10) + 1
        else:
            comment_page_count = int(int(diff_danawa_comment_count)/10)

        # 댓글 페이지 수에 따른 최대 Limit 수 조정
        dc.comment_page_limit = comment_page_count

        page = 1

        # Page 1부터 댓글 파싱
        while page <= dc.comment_page_limit:
            # page 1은 댓글 카운팅을 위해 요청하였으니 추가로 요청하지 않는다
            if not page == 1:
                danawa_review_html = dc.read_danawa_review(page=page)
                danawa_review_bs = BeautifulSoup(danawa_review_html.content, 'lxml')

            danawa_review_check = danawa_review_bs.find('div', {'class': 'danawa_review'})
            if danawa_review_check:
                comment_list = danawa_review_check.find('div', {'class': 'post_comments'})\
                    .find('ul')\
                    .find_all('li', id=re.compile('^danawa-prodBlog-productOpinion-list-self-\d+$'))

                danawa_comment_inner_list = dc.parsed_danawa_review(comment_list)
                comment_save(danawa_comment_inner_list, fid, collection)

            else:
                # 계산된 page 수 범위안에 있으나, 실제 댓글 페이지가 비어 있는 경우 Loop 탈출
                if 'NO_CONTENT' in danawa_review_bs.find('body').get_text(strip=True):
                    break

            page += 1
            # 크롤링 시 딜레이를 준다
            time.sleep(uniform(60, 65))

    # 상품 리뷰: 쇼핑몰 리뷰 파싱

    # 이 상품에 대한 저장된 댓글의 개수를 가져온다
    collection_mall_comment_count = collection.find({'fkey': fid, 'source': 'mall'}).count()

    if collection_mall_comment_count >= int(mall_review_count):
        pass
    else:
        diff_mall_comment_count = int(mall_review_count) - int(collection_mall_comment_count)

        # 댓글 크롤링을 위한 페이지 수 설정
        if int(diff_mall_comment_count) % 10 != 0:
            comment_page_count = int(int(diff_mall_comment_count)/10) + 1
        else:
            comment_page_count = int(int(diff_mall_comment_count)/10)

        # 댓글 페이지 수에 따른 최대 Limit 수 조정
        dc.comment_page_limit = comment_page_count

        # 오른쪽 부분의 상품 댓글 파싱
        # 댓글 페이지에서 최초 Page 1에 대한 댓글 파싱
        page = 1

        # Page 1부터 댓글 파싱
        while page <= dc.comment_page_limit:
            mall_review_html = dc.read_mall_review(page)
            mall_review_bs = BeautifulSoup(mall_review_html.content, 'lxml')

            comment_list = mall_review_bs.find('div', {'class': 'mall_review'})\
                .find('div', {'class': 'area_right'})\
                .find('ul', {'class': 'rvw_list'})\
                .find_all('li')

            danawa_comment_inner_list = dc.parsed_mall_review(comment_list)
            comment_save(danawa_comment_inner_list, fid, collection)

            page += 1
            time.sleep(uniform(60, 65))
