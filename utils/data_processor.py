import re
from typing import List, Dict
from config import Config
from utils.dictionary_handler import DictionaryHandler
from utils.file_reader import FileReader
from utils.logger import Logger


class DataProcessor:
    def __init__(self):
        self.config = Config()
        self.logger = Logger(__name__)
        self.dictionary_handler = DictionaryHandler(self.config.DICTIONARY_PATH)

    def process_data(self):
        """Основной метод обработки данных."""
        try:
            if self.config.USE_LOCAL_FILES:
                shop_products = FileReader.read_csv(self.config.SHOP_PRODUCTS_FILE)
                supplier_products = FileReader.read_csv(self.config.SUPPLIER_PRODUCTS_FILE)
            else:
                return []

            if not shop_products or not supplier_products:
                self.logger.error("Одна из таблиц пуста или не удалось загрузить данные.")
                return []

            self.logger.info(
                f"Загружено {len(shop_products)} товаров магазина и {len(supplier_products)} товаров поставщиков.")

            supplier_data = self._parse_supplier_products(supplier_products)
            self.logger.info(f"Обработано {len(supplier_data)} товаров поставщиков.")

            matched_products = []
            for shop_product in shop_products:
                if 'Наименование' not in shop_product:
                    self.logger.warning(f"Отсутствует столбец 'Наименование' в товаре: {shop_product}")
                    continue

                product_name = shop_product['Наименование']
                external_code = shop_product.get('Внешний код', 'N/A')

                product_dict = self.dictionary_handler.get_dictionary(product_name)
                self.logger.info(f"Создан словарь для товара: {product_name}, Ключевые слова: {product_dict}")

                matched_suppliers = self._match_suppliers(supplier_data, product_dict, product_name)
                self.logger.info(f"Найдено {len(matched_suppliers)} совпадений для товара: {product_name}")

                row = {
                    'Наше название': product_name,
                    'Внешний код': external_code
                }

                for i, supplier in enumerate(matched_suppliers, start=1):
                    row[f'Цена {i}'] = supplier['Цена']
                    row[f'Поставщик {i}'] = supplier['Поставщик']

                matched_products.append(row)

            self.logger.info("Обработка данных завершена успешно.")
            return matched_products

        except Exception as e:
            self.logger.error(f"Ошибка при обработке данных: {e}")
            return []

    def _parse_supplier_products(self, supplier_products: List[Dict]) -> List[Dict]:
        """Расширенный парсинг данных поставщиков."""
        supplier_data = []
        supplier_columns = ['поставщик', 'Поставщик', 'supplier', 'Supplier']
        name_columns = ['прайс', 'Наименование', 'название', 'name']

        for row in supplier_products:
            # Стратегия 1: Определение поставщика
            supplier = self._extract_supplier(row, supplier_columns)

            # Стратегия 2: Извлечение названия товара
            product_name = self._extract_product_name(row, name_columns)

            if not product_name or not supplier:
                continue

            # Стратегия 3: Извлечение цены
            price = self._extract_price(product_name)

            if price is None:
                continue

            # Дополнительные фильтры и обработка
            product_name = self._clean_product_name(product_name)

            # Расширенная валидация товара
            if not self._is_valid_product_advanced(product_name, supplier):
                continue

            supplier_data.append({
                'Поставщик': supplier,
                'Название': product_name,
                'Цена': price
            })

            self.logger.info(f"Добавлен товар: {product_name}, Цена: {price}, Поставщик: {supplier}")

        return supplier_data

    @staticmethod
    def _extract_supplier(row: Dict, supplier_columns: List[str]) -> str:
        """Извлечение поставщика с множественными стратегиями."""
        for col in supplier_columns:
            if col in row and row[col]:
                supplier = str(row[col]).strip()
                if supplier:
                    return supplier

        return 'Неизвестный'

    @staticmethod
    def _extract_product_name(row: Dict, name_columns: List[str]) -> str | None:
        """Извлечение названия товара с множественными стратегиями."""
        for col in name_columns:
            if col in row and row[col]:
                name = str(row[col]).strip()
                if name and len(name) > 3:
                    return name

        return None

    @staticmethod
    def _extract_price(product_name: str) -> int | None:
        """Извлечение цены из названия товара."""
        price_patterns = [
            r'\s(\d{4,5})\s*(?:₽|руб|rub|\$)?$',  # Число в конце строки
            r'(\d{4,5})\s*[₽$]',  # Число с валютой
            r'\b(\d{4,5})\b',  # Число между словами
        ]

        for pattern in price_patterns:
            match = re.search(pattern, product_name)
            if match:
                try:
                    price = int(match.group(1))
                    if 1000 <= price <= 200000:
                        return price
                except (ValueError, TypeError):
                    continue

        return None

    @staticmethod
    def _clean_product_name(product_name: str) -> str:
        """Очистка названия товара."""
        # Удаление эмодзи
        product_name = re.sub(r'[^\w\s()]', '', product_name)

        # Удаление лишних пробелов
        product_name = ' '.join(product_name.split())

        return product_name

    @staticmethod
    def _is_valid_product_advanced(product_name: str, supplier: str) -> bool:
        """Расширенная валидация товара с учетом поставщика."""
        # Список ключевых слов для разных поставщиков
        supplier_keywords = {
            'HI': ['iphone', 'airpods', 'ipad'],
            'DNS': ['смартфон', 'телефон'],
            'М.Видео': ['apple', 'samsung'],
        }

        # Общие стоп-слова
        stop_words = ['скидка', 'депозит', 'от \d+шт']

        # Проверка стоп-слов
        for word in stop_words:
            if re.search(word, product_name, re.IGNORECASE):
                return False

        # Проверка по keywords поставщика
        if supplier in supplier_keywords:
            return any(
                keyword.lower() in product_name.lower()
                for keyword in supplier_keywords[supplier]
            )

        return True

    def _match_suppliers(self, supplier_data: List[Dict], product_dict: List[str], product_name: str) -> List[Dict]:
        """Сопоставляет товары поставщиков с товарами магазина."""
        matched = []

        keywords = self._clean_keywords(product_name)

        memory_pattern = re.search(r'(\d+/\d+)\s*(?:GB|ГБ)', product_name, re.IGNORECASE)
        memory_config = memory_pattern.group(1) if memory_pattern else None

        potential_matches = []
        for supplier_product in supplier_data:
            supplier_name = supplier_product['Название'].lower()

            keyword_matches = sum(
                keyword in supplier_name
                for keyword in keywords
            )

            memory_match = (
                    memory_config and
                    memory_config in supplier_name
            ) if memory_config else False

            match_score = (
                    keyword_matches * 0.5 +
                    (memory_match * 2)
            )

            if match_score > 1:
                potential_matches.append({
                    'product': supplier_product,
                    'score': match_score
                })

        if potential_matches:
            sorted_matches = sorted(
                potential_matches,
                key=lambda x: x['score'],
                reverse=True
            )

            unique_prices = {}
            unique_suppliers = set()
            for match in sorted_matches:
                price = match['product']['Цена']
                supplier = match['product']['Поставщик']

                if price not in unique_prices and supplier not in unique_suppliers:
                    matched.append(match['product'])
                    unique_prices[price] = True
                    unique_suppliers.add(supplier)

                if len(matched) >= 3:
                    break

        return matched

    @staticmethod
    def _clean_keywords(product_name: str) -> List[str]:
        """Очищает ключевые слова от лишних символов."""
        cleaned = re.sub(r'[(),]', ' ', product_name)
        return [word.strip().lower() for word in cleaned.split() if word.strip()]
