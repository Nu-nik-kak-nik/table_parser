import re
from typing import List, Dict

from difflib import SequenceMatcher

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
                shop_products = self._load_shop_products()
                supplier_products = self._load_supplier_products()
            else:
                return []

            if not shop_products or not supplier_products:
                self.logger.error("Одна из таблиц пуста или не удалось загрузить данные.")
                return []

            self.logger.info(
                f"Загружено {len(shop_products)} товаров магазина и {len(supplier_products)} товаров поставщиков.")

            supplier_data = self._parse_supplier_products(supplier_products)
            self.logger.info(f"Обработано {len(supplier_data)} товаров поставщиков.")

            matched_products = self._match_products(shop_products, supplier_data)
            self.logger.info("Обработка данных завершена успешно.")
            return matched_products

        except Exception as e:
            self.logger.error(f"Ошибка при обработке данных: {e}")
            return []

    def _load_shop_products(self) -> List[Dict]:
        """Загрузка и обработка данных склада."""
        shop_products = FileReader.read_csv(self.config.SHOP_PRODUCTS_FILE)
        if not shop_products:
            self.logger.error("Таблица склада пуста или не удалось загрузить данные.")
            return []
        return [row for row in shop_products if any(row.values())]

    def _load_supplier_products(self) -> List[Dict]:
        """Загрузка и обработка данных поставщиков."""
        supplier_products = FileReader.read_csv(self.config.SUPPLIER_PRODUCTS_FILE)
        if not supplier_products:
            self.logger.error("Таблица поставщиков пуста или не удалось загрузить данные.")
            return []
        return [row for row in supplier_products if any(row.values())]

    def _match_products(self, shop_products: List[Dict], supplier_data: List[Dict]) -> List[Dict]:
        """Сопоставляет товары магазина с товарами поставщиков."""
        matched_products = []
        for shop_product in shop_products:
            if 'Наименование' not in shop_product:
                continue

            product_name = shop_product['Наименование']
            external_code = shop_product.get('Внешний код', 'N/A')

            product_dict = self.dictionary_handler.get_dictionary(product_name)
            matched_suppliers = self._match_suppliers(supplier_data, product_dict, product_name)

            row = {
                'Наше название': product_name,
                'Внешний код': external_code
            }

            # Добавляем всех подходящих поставщиков
            for i, supplier in enumerate(matched_suppliers, start=1):
                row[f'Цена {i}'] = supplier['Цена']
                row[f'Поставщик {i}'] = supplier['Поставщик']

            matched_products.append(row)

        return matched_products

    def _parse_supplier_products(self, supplier_products: List[Dict]) -> List[Dict]:
        supplier_data = []
        unique_products = set()  # Для отслеживания уникальных комбинаций

        supplier_columns = ['поставщик', 'Поставщик', 'supplier', 'Supplier']
        name_columns = [
            'прайс', 'Наименование', 'название', 'name',
            'Товар', 'product', 'Product', 'item'
        ]
        price_columns = ['Цена', 'цена', 'price', 'Price', 'стоимость']

        for row in supplier_products:
            # Расширенный поиск поставщика
            supplier = self._extract_supplier(row, supplier_columns)

            # Расширенный поиск названия
            product_name = self._extract_product_name(row, name_columns)

            if not product_name or not supplier:
                continue

            # Извлечение цены с ориентиром на конец строки
            price = self._extract_price_at_end(row, product_name)

            if price is None:
                continue

            # Очистка названия с сохранением информативности
            product_name = self._clean_product_name(product_name)

            # Более мягкая валидация
            if len(product_name) < 3:
                continue

            # Создаем уникальный ключ для фильтрации дубликатов
            product_key = f"{supplier}_{product_name}_{price}"

            # Добавляем только уникальные товары
            if product_key not in unique_products:
                unique_products.add(product_key)
                supplier_data.append({
                    'Поставщик': supplier,
                    'Название': product_name,
                    'Цена': price
                })

        self.logger.info(
            f"Обработано {len(supplier_products)} исходных товаров, оставлено {len(supplier_data)} уникальных.")
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
    def _extract_price(row: Dict, product_name: str) -> int | None:
        """Извлечение цены из названия товара или из отдельного столбца."""
        price_patterns = [
            r'\s(\d{4,5})\s*(?:₽|руб|rub|\$)?$',
            r'(\d{4,5})\s*[₽$]',
            r'\b(\d{4,5})\b',
        ]

        for pattern in price_patterns:
            match = re.search(pattern, product_name)
            if match:
                try:
                    price = int(match.group(1))
                    if 1000 <= price <= 300000:
                        return price
                except (ValueError, TypeError):
                    continue

        price_columns = ['Цена', 'цена', 'price', 'Price']
        for col in price_columns:
            if col in row and row[col]:
                try:
                    price = int(row[col])
                    if 1000 <= price <= 300000:
                        return price
                except (ValueError, TypeError):
                    continue

        return None

    @staticmethod
    def _clean_product_name(product_name: str) -> str:
        """Очищает название товара от лишних символов, сохраняя ключевые характеристики."""
        product_name = re.sub(r'[^\w\s/.-]', '', product_name)

        product_name = re.sub(r'\s*\d+(?:₽|$|🇰🇿)\s*', '', product_name)
        product_name = re.sub(r'🇺🇸|🇷🇺|🇪🇺|🇦🇪|🇮🇳|🇰🇿', '', product_name)

        product_name = re.sub(r'\s*-\s*', ' ', product_name)

        return ' '.join(product_name.split()).strip()

    @staticmethod
    def _is_valid_product_advanced(product_name: str, supplier: str) -> bool:
        supplier_keywords = {
            'HI': ['AirPods', 'iPhone', 'iPad', 'Watch', 'Mac', 'Samsung', 'Xiaomi'],
            'MiHonor': ['Samsung', 'Xiaomi', 'Redmi', 'Apple', 'iPhone', 'iPad'],
            'YouTakeAll': ['Google', 'Samsung', 'Pixel', 'Apple', 'iPhone', 'AirPods'],
            '112пав': ['iPhone', 'AirPods', 'iPad', 'Samsung', 'Xiaomi']
        }

        stop_words = ['скидка', 'депозит', r'от \d+шт', 'уценка']

        for word in stop_words:
            if re.search(word, product_name, re.IGNORECASE):
                return False

        if supplier in supplier_keywords:
            return any(
                keyword.lower() in product_name.lower()
                for keyword in supplier_keywords[supplier]
            )

        return len(product_name) > 3

    def _match_suppliers(self, supplier_data: List[Dict], product_dict: List[str], product_name: str) -> List[Dict]:
        """Сопоставляет товары поставщиков с товарами магазина."""
        matched = []
        keywords = self._clean_keywords(product_name)

        # Извлекаем характеристики (например, цвет, объём памяти)
        memory_pattern = re.search(r'(\d+/\d+)\s*(?:GB|ГБ)', product_name, re.IGNORECASE)
        memory_config = memory_pattern.group(1) if memory_pattern else None

        color_pattern = re.search(r'\b(black|white|blue|red|green|silver|gold)\b', product_name, re.IGNORECASE)
        color = color_pattern.group(0) if color_pattern else None

        for supplier_product in supplier_data:
            supplier_name = supplier_product['Название'].lower()

            # Используем расстояние Левенштейна для сравнения названий
            similarity = SequenceMatcher(None, product_name.lower(), supplier_name).ratio()

            # Считаем совпадения ключевых слов
            keyword_matches = sum(
                keyword in supplier_name
                for keyword in keywords
            )

            # Проверяем совпадение характеристик (память, цвет)
            memory_match = (
                    memory_config and
                    memory_config in supplier_name
            ) if memory_config else False

            color_match = (
                    color and
                    color in supplier_name
            ) if color else False

            # Вычисляем общий score
            match_score = (
                    similarity * 0.7 +  # Увеличиваем вес схожести
                    keyword_matches * 0.2 +  # Уменьшаем вес ключевых слов
                    (memory_match * 0.05) +  # Учитываем память
                    (color_match * 0.05)  # Учитываем цвет
            )

            if match_score > 0.6:  # Повышаем порог
                matched.append(supplier_product)

        return matched

    @staticmethod
    def _clean_keywords(product_name: str) -> List[str]:
        """Очищает ключевые слова от лишних символов и разделяет значения по слешу."""
        product_name = re.sub(r'\s\d{4,5}\s*(?:₽|руб|rub|\$)?$', '', product_name)

        # Удаляем "+", если он окружен пробелами
        product_name = re.sub(r'\s\+\s', ' ', product_name)

        parts = re.split(r'[/]', product_name)
        keywords = []
        for part in parts:
            part = re.sub(r'[^\w\s.+]', '', part)
            keywords.extend([word.strip().lower() for word in part.split() if word.strip()])

        # Добавляем синонимы
        synonyms = {
            'type-c': 'usb-c',
            'wi-fi': 'wifi',
            'cellular': 'lte',
            'iphone': 'apple',
            'ipad': 'apple',
            'airpods': 'apple'
        }
        keywords = [synonyms.get(word, word) for word in keywords]

        return keywords

    @staticmethod
    def _extract_price_at_end(row: Dict, product_name: str) -> int | None:
        """Извлечение цены с ориентиром на конец строки."""
        price_patterns = [
            r'(\d{4,5})\s*(?:₽|руб|rub|\$)?$',  # Цена в конце строки
            r'\s(\d{4,5})\s*(?:₽|руб|rub|\$)$',  # С пробелом перед ценой
        ]

        for pattern in price_patterns:
            match = re.search(pattern, product_name)
            if match:
                try:
                    price = int(match.group(1))
                    if 1000 <= price <= 300000:
                        # Удаляем цену из названия
                        product_name = re.sub(pattern, '', product_name).strip()
                        return price
                except (ValueError, TypeError):
                    continue

        # Резервный метод извлечения цены из столбцов
        price_columns = ['Цена', 'цена', 'price', 'Price']
        for col in price_columns:
            if col in row and row[col]:
                try:
                    price = int(str(row[col]).replace(' ', ''))
                    if 1000 <= price <= 300000:
                        return price
                except (ValueError, TypeError):
                    continue

        return None