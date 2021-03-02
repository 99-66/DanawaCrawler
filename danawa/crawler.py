import ast
import datetime
import re
import time

import requests
import hjson
from urllib.parse import urlparse, quote, parse_qsl
from utils.user_agent import random_user_agent
from utils.proxy import RandProxy

import config


class DanawaSearcher:
    """
    다나와 상품 검색을 하기 위한 클래스
    """
    def __init__(self):
        # Headers set
        self.proxy = RandProxy()
        self.user_agent = random_user_agent()
        self.url_set = config.URL
        self.origin = None
        self.host = None

        # Searcher
        self.limit = 1

    @staticmethod
    def _referer(keyword) -> str:
        """
        requests header 에 포함할 referer 값을 반환한다
        """
        return f'http://search.danawa.com/dsearch.php?query={quote(keyword)}&tab=main'

    def _init_headers(self) -> None:
        """
        검색 URL 을 기준으로 이후 검색에 사용할 request header 를 설정한다
        """
        search_url = self.url_set['search']
        parsed_url = urlparse(search_url)

        self.origin = f'{parsed_url.scheme}://{parsed_url.netloc}'
        self.host = parsed_url.netloc

    def _headers(self, keyword) -> dict:
        return {
            'User-Agent': self.user_agent,
            'Accept': 'text/html, */*; q=0.01',
            'Accept-Encoding': 'gzip, deflate',
            'Accept-Language': 'ko-KR,ko;q=0.8,en-US;q=0.5,en;q=0.3',
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive',
            'Content-Type': 'application/x-www-form-urlencoded',
            'Origin': self.origin,
            'Referer': self._referer(keyword),
            'Host': self.host,
            'Pragma': 'no-cache',
            'X-Requested-With': 'XMLHttpRequest'
        }

    @staticmethod
    def search_parameter(keyword: str, page: int = 1,) -> dict:
        """
        검색에 사용할 post parameter 를 생성하여 반환한다
        """
        return {
            'query': keyword,
            'originalQuery': keyword,
            'previousKeyword': keyword,
            'volumeType': 'vmvs',
            'page': page,
            'limit': 90,
            'sort': 'saveDESC',
            'list': 'list',
            'boost': 'true',
            'addDelivery': 'N',
            'tab': 'goods'
        }

    @staticmethod
    def length_search_parameter(search_parameter: dict) -> str:
        """
        requests header 에 포함할 content-length 값을 계산하여 반환한다
        """
        parameters = ''
        for k, v in search_parameter.items():
            parameters += f'{k}={v}&'
        parameters = parameters.rstrip('&')

        return str(len(parameters))

    def fetch(self, keyword, page=1) -> requests:
        self._init_headers()
        proxies = self.proxy.get()
        data = self.search_parameter(keyword, page)
        headers = self._headers(keyword)
        headers['Content-Length'] = self.length_search_parameter(data)

        response = requests.post(self.url_set['search'], data=data, proxies=proxies, headers=headers, timeout=15)
        response.raise_for_status()

        return response.content


class DanawaCrawler:
    """
    다나와 상품 정보와 댓글을 크롤링 하기 위한 클래스
    """
    def __init__(self):
        self.proxy = RandProxy()
        self.user_agent = random_user_agent()
        self.current_navigation_pattern = re.compile('var\s+oCurrentNavigation\s+=\s+(.*?);')
        self.price_compare_pattern = re.compile('var\s+oPriceCompareSetting\s+=\s+(.*?);')
        self.product_description_pattern = re.compile('var\s+oProductDescriptionInfo\s+=\s+(.*?);')
        self.global_setting_pattern = re.compile('var\s+oGlobalSetting\s+=\s+(.*?);')
        self.physical_category_name_list_pattern = re.compile('var\s+oPhysicalCategoryNameList\s+=\s+(.*?);')
        self.url_set = config.URL
        self.referer = None
        self.header_host = None
        self.origin = None
        self.limit = 10
        self.comment_page_limit = 10
        self.pcode = None
        # cate = nGroup + nDepth + ?
        self.category = {}
        self.cate = None
        self.cate1 = None
        self.cate2 = None
        self.cate3 = None
        self.cate4 = None

    def _headers(self, method: str) -> dict:
        if method.upper() == 'GET':
            return {
                # 'User-Agent': self.user_agent,
                #  Requests encoding 문제로 인해 특정 user-agent 로 고정한다
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_2) '
                              'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.88 Safari/537.36',
                'Accept': '*/*',
                'Accept-Encoding': 'gzip, deflate',
                'Accept-Language': 'ko-KR,ko;q=0.8,en-US;q=0.5,en;q=0.3',
                'Cache-Control': 'no-cache',
                'Connection': 'keep-alive',
                'Content-Type': 'application/x-www-form-urlencoded;charset=UTF-8',
                'Host': self.header_host,
                'Referer': self.referer,
                'Pragma': 'no-cache',
            }
        elif method.upper() == 'POST':
            return {
                'User-Agent': self.user_agent,
                'Accept': 'text/html, */*; q=0.01',
                'Accept-Encoding': 'gzip, deflate',
                'Accept-Language': 'ko-KR,ko;q=0.8,en-US;q=0.5,en;q=0.3',
                'Cache-Control': 'no-cache',
                'Connection': 'keep-alive',
                'Content-Type': 'application/x-www-form-urlencoded;charset=UTF-8',
                'Origin': self.origin,
                'Referer': self.referer,
                'Host': self.header_host,
                'Pragma': 'no-cache',
                'X-Requested-With': 'XMLHttpRequest'
            }

    @staticmethod
    def _script_to_json(script: str) -> dict or bool:
        data = hjson.loads(script)
        if data:
            return dict(data)
        else:
            return False

    @staticmethod
    def _timestamp():
        return int(datetime.datetime.now().timestamp()*1000)

    def _init_url_parse(self, url: str) -> None:
        """
        최초 상품 페이지의 URL 을 기준으로 Request header 정보를 설정
        """
        parsed_url = urlparse(url)
        parted_query_string = dict(parse_qsl(parsed_url.query))

        self.header_host = parsed_url.netloc
        self.origin = f'{parsed_url.scheme}://{parsed_url.netloc}'

        s = ''
        for k, v in parted_query_string.items():
            if k == 'keyword':
                s += f'{k}={quote(v)}'
            else:
                s += f'{k}={v}'
        self.referer = f'{self.origin}/{s}'

        try:
            self.pcode = parted_query_string['pcode']
        except KeyError:
            self.pcode = None

        try:
            self.cate = parted_query_string['cate']
        except KeyError:
            self.cate = None

    def _set_param_global_setting(self, global_setting: dict, current_navigation: dict) -> None:
        if global_setting:
            # CategoryCode Set
            try:
                self.cate = global_setting['nCategoryCode']
            except KeyError:
                pass

            try:
                self.cate1 = global_setting['nCategoryCode1']
            except KeyError:
                pass

            try:
                self.cate2 = global_setting['nCategoryCode2']
            except KeyError:
                pass

            try:
                self.cate3 = global_setting['nCategoryCode3']
            except KeyError:
                pass

            try:
                self.cate4 = global_setting['nCategoryCode4']
            except KeyError:
                pass

        # Category Set
        if global_setting and current_navigation:
            self.category['GroupName'] = global_setting['sGroupName']
            for key, value in current_navigation.items():
                self.category[key] = {
                    'code': value['code'],
                    'name': value['name'],
                    'parent': value['parent'],
                    'group': value['group'],
                    'depth': value['depth']
                }

    def _set_param_product_description(self, ret):
        obj = {
            'pcode': self.pcode,
            'cate1': self.cate1,
            'cate2': self.cate2,
            'cate3': self.cate3,
            'cate4': self.cate4,
            'productFullName': ret['price_compare']['sProductFullName']
        }
        ret['product_description'].update(obj)

        if 'productFullName' in ret['product_description']:
            ret['product_description']['productFullName'] = ret['product_description']['productFullName'].replace(' ', '+')

        if 'productName' in ret['product_description']:
            ret['product_description']['productName'] = ret['product_description']['productName'].replace(' ', '+')

        if 'makerName' in ret['product_description']:
            ret['product_description']['makerName'] = ret['product_description']['makerName'].replace(' ', '+')

        return ret['product_description']

    def _set_param_price_compare(self, ret):
        obj = {
            'pcode': self.pcode,
            'cate1': self.cate1,
            'cate2': self.cate2,
            'cate3': self.cate3,
            'cate4': self.cate4,
        }
        ret['price_compare'].update(obj)
        if 'sProductFullName' in ret['price_compare']:
            ret['price_compare']['sProductFullName'] = ret['price_compare']['sProductFullName'].replace(' ', '+')

        return ret['price_compare']

    # 상품 페이지의 HTML 을 가져온다
    def read(self, url) -> requests:
        self._init_url_parse(url)
        headers = self._headers('GET')

        # 최초 요청 시에만 Header 를 일부 수정
        headers['Accept'] = 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
        headers['Upgrade-Insecure-Requests'] = '1'
        del headers['Referer']
        del headers['Content-Type']
        proxies = self.proxy.get()

        res = requests.get(url, headers=headers, proxies=proxies)
        res.raise_for_status()

        return res

    def _is_current_navigation(self, scripted):
        current_navigation_script = self.current_navigation_pattern.search(scripted)

        if current_navigation_script:
            return self._script_to_json(current_navigation_script.group(1))

    def _is_product_description(self, scripted):
        production_desc_script = self.product_description_pattern.search(scripted)
        if production_desc_script:
            return self._script_to_json(production_desc_script.group(1))

    def _is_price_compare(self, scripted):
        price_compare_script = self.price_compare_pattern.search(scripted)
        if price_compare_script:
            return self._script_to_json(price_compare_script.group(1))

    def _is_global_setting(self, scripted):
        global_setting_script = self.global_setting_pattern.search(scripted)
        if global_setting_script:
            return self._script_to_json(global_setting_script.group(1))

    def _is_physical_category(self, scripted):
        physical_category_script = self.physical_category_name_list_pattern.search(scripted)
        if physical_category_script:
            cate_list = physical_category_script.group(1)
            # string list 를 list 타입으로 변경한다
            cate_list = ast.literal_eval(cate_list)
            # escape 문자열을 삭제한다
            cate_list = [i.replace('\\', '').strip() for i in cate_list]
            return cate_list

    # 상품 페이지의 내용 중 script 에 포함된 'oPriceCompareSetting', 'oProductDescriptionInfo',
    # 'oGlobalSetting', 'oCurrentNavigation' 변수를 dictionary 로 설정
    # 'oPriceCompareSetting' : 가격비교 요청을 위한 Parameter 설정에 사용
    # 'oProductDescriptionInfo' : 상품 상세정보 요청을 위한 Parameter 설정에 사용
    # 'oGlobalSetting' : cate1 ~ 4 번호는 추가 변수 설정에 사용
    # 'oCurrentNavigation' : Category name 과 no 를 저장하기 위해 사용
    # script_data =  bs.find_all('script', {'src': False})
    # parsed_html_script(script_data)
    def parsed_html_script(self, script_data) -> dict:
        ret = {
            'current_navigation': {},
            'product_description': {},
            'price_compare': {},
            'global_setting': {},
            'physical_category': {}
        }

        for script in script_data:
            scripted = script.string.replace('\r', '').replace('\n', '').replace('\t', '')
            if not ret['current_navigation']:
                ret['current_navigation'] = self._is_current_navigation(scripted)

            if not ret['global_setting']:
                ret['global_setting'] = self._is_global_setting(scripted)

            if not ret['product_description']:
                ret['product_description'] = self._is_product_description(scripted)

            if not ret['price_compare']:
                ret['price_compare'] = self._is_price_compare(scripted)
            if not ret['physical_category']:
                ret['physical_category'] = self._is_physical_category(scripted)

        self._set_param_global_setting(ret['global_setting'], ret['current_navigation'])
        ret['product_description'] = self._set_param_product_description(ret)
        ret['price_compare'] = self._set_param_price_compare(ret)

        # 이후 사용하지 않으므로 삭제 후 리턴
        del ret['current_navigation']
        del ret['global_setting']

        return ret

    def read_price_compare(self, parameter: dict) -> requests:
        url = self.url_set['price_compare']

        headers = self._headers('POST')
        proxies = self.proxy.get()

        res = requests.post(url, data=parameter, headers=headers, proxies=proxies)
        res.raise_for_status()

        return res

    def read_danawa_review(self, page=1) -> requests:
        url = self.url_set['danawa_review'].format(prodcode=self.pcode, page=page,
                                                   limit=self.limit, timestamp=self._timestamp())
        headers = self._headers('GET')
        proxies = self.proxy.get()

        # 헤더 문제
        res = requests.get(url, headers=headers, proxies=proxies)
        res.raise_for_status()

        return res

    def read_mall_review(self, page=1) -> requests:
        url = self.url_set['mall_review'].format(prodcode=self.pcode, page=page, limit=self.limit, cate1=self.cate1,
                                                 timestamp=self._timestamp())
        headers = self._headers('GET')
        proxies = self.proxy.get()

        res = requests.get(url, headers=headers, proxies=proxies)
        res.raise_for_status()

        return res

    @staticmethod
    def parsed_price_list(group: str, price_list_item: str) -> list:
        # Not Use
        # Option 파싱 구분을 위해 group 을 추가로 받는다
        # 샵다나와 관련 항목의 option 은 구조가 다르므로 조건 처리

        price_inner_list = []

        for detail_rank, price_list in enumerate(price_list_item, 1):
            try:
                mall = price_list.find('div', {'class': 'd_mall'}).find('img')['alt'].strip()
            except AttributeError:
                mall = None
            except TypeError:
                try:
                    mall = price_list.find('div', {'class': 'd_mall'})\
                        .find('span', {'class': 'txt_logo'})\
                        .get_text(strip=True)
                except AttributeError:
                    mall = None

            # 일반 전문몰에 첫번째는 삽댜나와 딜러로 가격정보가 없으므로 다음으로 넘김
            if group == '일반_전문몰' and mall == '샵다나와':
                continue

            try:
                price = price_list.find('div', {'class': 'd_dsc'}) \
                    .find('div', {'class': 'prc_line'}) \
                    .find('a') \
                    .find('em') \
                    .get_text()
            except AttributeError:
                price = None
            else:
                price = price.replace(',', '').replace('원', '')

            try:
                shipping = price_list.find('div', {'class': 'd_dsc'}) \
                    .find('div', {'class': 'prc_line'}) \
                    .find('span', {'class': 'ship'}) \
                    .get_text() \
                    .strip()
            except AttributeError:
                shipping = None
            else:
                shipping = shipping.replace(',', '').replace('원', '') \
                    .replace('(', '').replace(')', '').replace('배송비', '').strip()

            try:
                description = price_list.find('div', {'class': 'info_line'}).find('a').get_text()
            except AttributeError:
                description = None

            try:
                if group.startswith('샵다나와'):
                    option = price_list.find('div', {'class': 'd_dsc'}) \
                        .find('div', {'class': 'etc_line'}) \
                        .find('span', {'class': 'txt'}) \
                        .get_text() \
                        .strip()
                else:
                    option = price_list.find('div', {'class': 'd_dsc'}) \
                        .find('div', {'class': 'etc_line'}) \
                        .find('a', {'class': 'txt'}) \
                        .get_text() \
                        .strip()
            except AttributeError:
                option = None

            obj = {
                'mall': mall,
                'price': price,
                'shipping': shipping,
                'description': description,
                'option': option,
                'detail_rank': detail_rank,
            }
            price_inner_list.append(obj)

        return price_inner_list

    @staticmethod
    def _parsed_danawa_comment(comment) -> dict:
        try:
            nickname = comment.find('div', {'class': 'cont_area'}) \
                .find('div', {'class': 'r_info'}) \
                .find('div', {'class': 'user_info'}) \
                .find('a', {'class': 'id_name danawa-prodBlog-memberInfo-clazz'}) \
                .strong \
                .get_text() \
                .strip()
        except AttributeError:
            nickname = None

        try:
            comment_date = comment.find('div', {'class': 'cont_area'}) \
                .find('div', {'class': 'r_info'}) \
                .find('span', {'class': 'date'}) \
                .get_text() \
                .strip()
        except AttributeError:
            comment_date = None
        else:
            comment_date = int(time.mktime(datetime.datetime.strptime(comment_date, '%Y.%m.%d %H:%M:%S').timetuple()))

        try:
            comment_ip = comment.find('div', {'class': 'cont_area'}) \
                .find('div', {'class': 'r_info'}) \
                .find('span', {'class': 'ip'}) \
                .get_text() \
                .strip()
        except AttributeError:
            comment_ip = None
        try:
            comment_text = comment.find('div', {'class': 'cont_area'}) \
                .find('div', id=re.compile('^danawa-prodBlog-productOpinion-list-wrap-\d+$')) \
                .find('div', id=re.compile('^danawa-prodBlog-productOpinion-content-text-\d+$'))
        except AttributeError:
            comment_text = None
        else:
            if comment_text:
                comment_text = comment_text.get_text().strip()
            else:
                comment_text = None

        try:
            comment_recommend = comment.find('div', {'class': 'cont_area'}) \
                .find('div', id=re.compile('^danawa-prodBlog-productOpinion-list-wrap-\d+$')) \
                .find('button', id=re.compile('^danawa-prodBlog-productOpinion-button-recommend-\d+$'))
        except:
            comment_recommend = 0
        else:
            if comment_recommend:
                comment_recommend = comment_recommend.find('span', {'class': 'num_c'}).get_text(strip=True)
                if comment_recommend:
                    comment_recommend = int(comment_recommend)
                else:
                    comment_recommend = 0
            else:
                comment_recommend = 0

        return {
            'userName': nickname,
            'publishedAtTimestamp': comment_date,
            'userIp': comment_ip,
            'contentText': comment_text,
            'likeCount': comment_recommend,
            'review': 'danawa'
        }

    def parsed_danawa_review(self, comment_list) -> list:
        comment_ret_list = []

        for comment in comment_list:
            # 대댓글이 있는 경우에는 sub_item 을 포함하고 있으므로, 체크한 후 처리하지 않고 넘긴다
            if 'sub_item' in comment.get('class'):
                continue

            comment_content = self._parsed_danawa_comment(comment)
            comment_ret_list.append(comment_content)

        return comment_ret_list

    @staticmethod
    def parsed_mall_review(comment_list) -> list:
        comment_ret_list = []

        for comment in comment_list:
            try:
                if not comment.get('id').startswith('danawa-prodBlog'):
                    # 리뷰가 아닌 li 태그가 검색되어 Loop 에 포함된 경우에는 무시한다
                    continue
            except AttributeError:
                continue

            try:
                user_point = comment.find('div', {'class': 'top_info'}).find('span', {'class': 'star_mask'}).get_text()
            except AttributeError:
                user_point = None
            else:
                user_point = user_point.replace('점', '')

            try:
                comment_date = comment.find('div', {'class': 'top_info'}).find('span', {'class': 'date'}).get_text().strip()
            except AttributeError:
                comment_date = None
            else:
                comment_date = int(time.mktime(datetime.datetime.strptime(comment_date, '%Y.%m.%d').timetuple()))

            try:
                buy_mall = comment.find('div', {'class': 'top_info'}).find('span', {'class': 'mall'}).get_text().strip()
            except AttributeError:
                buy_mall = None

            try:
                nickname = comment.find('div', {'class': 'top_info'}).find('span', {'class': 'name'}).get_text().strip()
            except AttributeError:
                nickname = None

            try:
                comment_title = comment.find('div', {'class': 'rvw_atc'}).find('div', {'class': 'tit_W'}).find('p').get_text().strip()
            except AttributeError:
                comment_title = None

            try:
                comment_text = comment.find('div', {'class': 'rvw_atc'}).find('div', {'class': 'atc'}).get_text().strip()
            except AttributeError:
                comment_text = None

            obj = {
                'ratingPoint': user_point,
                'publishedAtTimestamp': comment_date,
                'userName': nickname,
                'review': buy_mall,
                'contentTitle': comment_title,
                'contentText': comment_text
            }
            comment_ret_list.append(obj)

        return comment_ret_list
