import os

DEBUG = False

# Crawling URL Formatter
URL = {
    'search': 'http://search.danawa.com/ajax/getProductList.ajax.php',
    'danawa_review': 'http://prod.danawa.com/info/dpg/ajax/productOpinion.ajax.php?&prodCode={prodcode}'
                     '&keyword=&condition=&page={page}&limit={limit}&past=N&_={timestamp}',
    'mall_review': 'http://prod.danawa.com/info/dpg/ajax/companyProductReview.ajax.php?prodCode={prodcode}'
                   '&cate1Code={cate1}&page={page}&limit={limit}&score=0&sortType=NEW&usefullScore=Y&innerKeyword='
                   '&subjectWord=0&subjectWordString=&subjectSimilarWordString=&_={timestamp}',
    'price_compare': 'http://prod.danawa.com/info/ajax/getAllPriceCompareMallList.ajax.php',
    'product_description': 'http://prod.danawa.com/info/ajax/getProductDescription.ajax.php'
}

# Redis Configuration
REDIS = {
    'HOST': '',
    'PASSWORD': '',
    'PORT': 6379,
    'DB': 1
}


# MongoDB Configuration
MONGODB = {
    'HOST': '',
    'USER': '',
    'PASSWORD': '',
    'PORT': '27017',
    'SSL': True,
    'SSL_CA_CERTS': '',
    'REPLICA_SET': '',
    'COLLECTION': ''
}

# Logging Configuration
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_CFG = os.path.join(BASE_DIR, 'logging.json')
LOG_PATH = os.path.join(BASE_DIR, 'logs')
LOG_FILENAME = f'{LOG_PATH}/danawa_crawling.log'

if not os.path.exists(LOG_PATH):
    os.mkdir(LOG_PATH)


