from dataclasses import dataclass


@dataclass
class Config:
    SHOP_PRODUCTS_URL = 'Uhttps://docs.google.com/spreadsheets/d/1UzD7cHRV_GSq_l-t_uaApOUhG6KcKZHVMgpTPJEdydU/edit?gid=97610138#gid=97610138'
    SUPPLIER_PRODUCTS_URL = 'https://docs.google.com/spreadsheets/d/1F4EddLaQQtI-8SlzaNZHxXMYQgB7cGHzB1clCHOwMMw/edit?gid=0#gid=0'

    USE_LOCAL_FILES = True  # Переключение между Google Sheets и локальными файлами не доработано
    SHOP_PRODUCTS_FILE = 'data/shop_products.csv'
    SUPPLIER_PRODUCTS_FILE = 'data/supplier_products.csv'
    OUTPUT_FILE: str = 'matched_products.csv'

    OUTPUT_DICT = 'output'
    OUTPUT_PATH = 'output/matched_products.csv'
    DICTIONARY_PATH = 'data/dictionaries.json'

    LOG_FILE = 'logs/app.log'
    LOG_LEVEL: str = 'INFO'

    MATCHING_THRESHOLD: float = 0.7

    SHOP_NAME_COLUMN: str = 'Наименование'
    SHOP_CODE_COLUMN: str = 'Внешний код'

    SUPPLIER_PRICE_COLUMN: str = 'прайс'

    STOP_WORDS = {
        'смартфон', 'планшет', 'телефон', 'часы', 'watch', 'phone',
        'smartphone', 'tablet', 'mobile', 'мобильный',

        'в', 'из', 'для', 'с', 'по', 'от',

        'gb', 'гб', 'ram', 'rom'
    }