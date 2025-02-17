import difflib
from typing import List, Dict
from .dictionary_manager import DictionaryManager


class ProductMatcher:
    def __init__(self, config):
        self.config = config

    def match_products(self, shop_products: List[Dict], suppliers_products: List[Dict]) -> List[Dict]:
        matched_products = []

        for shop_product in shop_products:
            # Генерация ключевых слов
            shop_keywords = DictionaryManager.create_product_keywords(shop_product)

            # Поиск поставщиков
            suppliers = self._find_suppliers(shop_keywords, suppliers_products)

            # Формирование результата
            result = {
                **shop_product,
                'suppliers': suppliers
            }
            matched_products.append(result)

        return matched_products

    def _find_suppliers(self, shop_keywords: List[str], suppliers_products: List[Dict]) -> List[Dict]:
        matched_suppliers = []

        for supplier_product in suppliers_products:
            # Генерация ключевых слов для товара поставщика
            supplier_keywords = [
                supplier_product.get('Название', '').lower(),
                str(supplier_product.get('Цена', '')).lower()
            ]

            # Расчет совпадения
            matches = [
                difflib.SequenceMatcher(None, kw1, kw2).ratio()
                for kw1 in shop_keywords
                for kw2 in supplier_keywords
            ]

            max_match = max(matches) if matches else 0

            if max_match >= self.config.MATCH_THRESHOLD:
                matched_suppliers.append({
                    'name': supplier_product.get('Название поставщика', ''),
                    'price': supplier_product.get('Цена', 0)
                })

        # Сортировка и ограничение количества поставщиков
        matched_suppliers.sort(key=lambda x: x['price'])
        return matched_suppliers[:self.config.MAX_SUPPLIERS_PER_PRODUCT]
