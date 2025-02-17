import re

from config import Config
from utils.dictionary_handler import DictionaryHandler
from utils.file_reader import FileReader
from utils.google_sheets import GoogleSheetsHandler
from utils.logger import Logger


class DataProcessor:
    def __init__(self):
        self.config = Config()
        self.logger = Logger(__name__)
        self.dictionary_handler = DictionaryHandler(self.config.DICTIONARY_PATH)

    def process_data(self):
        """Основной метод обработки данных."""
        try:
            # Чтение данных
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

            # Обработка данных поставщиков
            supplier_data = self._parse_supplier_products(supplier_products)
            self.logger.info(f"Обработано {len(supplier_data)} товаров поставщиков.")

            # Сопоставление товаров
            matched_products = []
            for shop_product in shop_products:
                if 'Наименование' not in shop_product:
                    self.logger.warning(f"Отсутствует столбец 'Наименование' в товаре: {shop_product}")
                    continue

                product_name = shop_product['Наименование']
                external_code = shop_product.get('Внешний код', 'N/A')

                # Используем метод get_dictionary из dictionary_handler
                product_dict = self.dictionary_handler.get_dictionary(product_name)
                self.logger.info(f"Создан словарь для товара: {product_name}, Ключевые слова: {product_dict}")

                # Передаем product_name в _match_suppliers
                matched_suppliers = self._match_suppliers(supplier_data, product_dict, product_name)
                self.logger.info(f"Найдено {len(matched_suppliers)} совпадений для товара: {product_name}")

                # Формирование строки итоговой таблицы
                row = {
                    'Наше название': product_name,
                    'Внешний код': external_code
                }

                # Добавляем данные поставщиков
                for i, supplier in enumerate(matched_suppliers, start=1):
                    row[f'Цена {i}'] = supplier['Цена']
                    row[f'Поставщик {i}'] = supplier['Поставщик']

                matched_products.append(row)

            self.logger.info("Обработка данных завершена успешно.")
            return matched_products

        except Exception as e:
            self.logger.error(f"Ошибка при обработке данных: {e}")
            return []

    @staticmethod
    def _is_valid_product(product_name: str) -> bool:
        """Проверяет, является ли строка корректным товаром."""
        # Исключаем строки, содержащие номера телефонов, депозиты и т.д.
        invalid_patterns = [
            r'\+7-\d{3}-\d{3}-\d{2}-\d{2}',  # Номер телефона
            r'Депозит',  # Депозит
            r'Скидка',  # Скидка
        ]

        # Проверяем, что это похоже на смартфон
        smartphone_patterns = [
            r'\b(смартфон|телефон)\b',
            r'\b(samsung|iphone|xiaomi|honor)\b',
            r'\b(galaxy|z fold|fold)\b'
        ]

        for pattern in invalid_patterns:
            if re.search(pattern, product_name, re.IGNORECASE):
                return False

        # Проверяем, что это точно смартфон
        return any(
            re.search(pattern, product_name, re.IGNORECASE)
            for pattern in smartphone_patterns
        )

    def _parse_supplier_products(self, supplier_products):
        """Парсит данные поставщиков и возвращает список словарей с товарами."""
        supplier_data = []
        current_supplier = None

        for row in supplier_products:
            # Обработка различных вариантов наименования столбцов
            product_name = (
                    row.get('прайс', '') or
                    row.get('Наименование', '') or
                    row.get('название', '')
            ).strip()

            # Пропускаем пустые строки
            if not product_name:
                continue

            # Специальная обработка для AirPods и других электронных устройств
            if product_name.startswith('🎧'):
                # Удаляем эмодзи и лишние пробелы
                product_name = product_name.replace('🎧', '').strip()

            # Извлечение поставщика
            supplier = (
                    row.get('поставщик', '') or
                    row.get('Поставщик', '') or
                    current_supplier or
                    'Неизвестный'
            ).strip()

            # Попытка извлечь цену
            price = self._extract_price(product_name)

            # Если цена не найдена, пробуем найти в отдельном столбце
            if price is None:
                price_columns = ['цена', 'Цена', 'price']
                for col in price_columns:
                    if col in row and row[col]:
                        try:
                            price = int(row[col])
                            break
                        except ValueError:
                            continue

            # Если цена всё еще не найдена, пропускаем товар
            if price is None:
                self.logger.warning(f"Не удалось извлечь цену для товара: {product_name}")
                continue

            # Проверка на валидность товара с расширенной логикой
            if not self._is_valid_product(product_name):
                # Добавляем специальную обработку для AirPods и других устройств
                if 'airpods' not in product_name.lower():
                    self.logger.warning(f"Пропущен некорректный товар: {product_name}")
                    continue

            # Добавляем товар в список
            supplier_data.append({
                'Поставщик': supplier,
                'Название': product_name,
                'Цена': price
            })
            self.logger.info(f"Добавлен товар: {product_name}, Цена: {price}, Поставщик: {supplier}")

        return supplier_data

    @staticmethod
    def _is_supplier_header(product_name):
        """Проверяет, является ли строка заголовком поставщика."""
        return '[' in product_name and ']' in product_name

    @staticmethod
    def _extract_supplier_name(supplier_name):
        """Извлекает название поставщика из строки."""
        return supplier_name.strip()

    @staticmethod
    def _is_product_with_price(product_name):
        """Проверяет, содержит ли строка товар и цену."""
        return any(char.isdigit() for char in product_name)

    @staticmethod
    def _extract_price(product_name: str):
        """Извлекает цену из названия товара поставщика."""
        price_patterns = [
            r'(\d{4,5})\s*[₽$]',
            r'\s(\d{4,5})\s',
            r'[^\d](\d{4,5})$'
        ]

        for pattern in price_patterns:
            match = re.search(pattern, product_name)
            if match:
                try:
                    price = int(match.group(1))
                    if 1000 <= price <= 2000000:
                        return price
                except (ValueError, TypeError):
                    continue

    def _match_suppliers(self, supplier_data, product_dict, product_name):
        """Сопоставляет товары поставщиков с товарами магазина."""
        matched = []

        # Извлекаем ключевые слова
        keywords = self._clean_keywords(product_name)

        # Извлечение конфигурации памяти
        memory_pattern = re.search(r'(\d+/\d+)\s*(?:GB|ГБ)', product_name, re.IGNORECASE)
        memory_config = memory_pattern.group(1) if memory_pattern else None

        potential_matches = []
        for supplier_product in supplier_data:
            supplier_name = supplier_product['Название'].lower()

            # Расширенная проверка ключевых слов
            keyword_matches = sum(
                keyword in supplier_name
                for keyword in keywords
            )

            # Проверка памяти
            memory_match = (
                    memory_config and
                    memory_config in supplier_name
            ) if memory_config else False

            # Оценка совпадения
            match_score = (
                    keyword_matches * 0.5 +  # Вес ключевых слов
                    (memory_match * 2)  # Дополнительный вес за память
            )

            if match_score > 1:  # Настраиваемый порог совпадения
                potential_matches.append({
                    'product': supplier_product,
                    'score': match_score
                })

        # Сортировка и фильтрация совпадений (как в предыдущей версии)
        if potential_matches:
            sorted_matches = sorted(
                potential_matches,
                key=lambda x: x['score'],
                reverse=True
            )

            # Выбор уникальных цен
            unique_prices = {}
            unique_suppliers = set()
            for match in sorted_matches:
                price = match['product']['Цена']
                supplier = match['product']['Поставщик']

                if price not in unique_prices and supplier not in unique_suppliers:
                    unique_prices[price] = match['product']
                    unique_suppliers.add(supplier)
                    matched.append({
                        'Поставщик': supplier,
                        'Цена': price
                    })

                if len(matched) >= 5:
                    break

        # Логирование результатов
        self.logger.info(f"Найдено {len(matched)} совпадений для товара: {product_name}")
        for m in matched:
            self.logger.info(f"Поставщик: {m['Поставщик']}, Цена: {m['Цена']}")

        return matched

    @staticmethod
    def _contains_all_keywords(supplier_product_name, product_dict, threshold=0.5):
        """Проверяет, содержит ли товар поставщика достаточно ключевых слов."""
        matched_keywords = sum(keyword in supplier_product_name for keyword in product_dict)
        return matched_keywords / len(product_dict) >= threshold

    @staticmethod
    def _clean_keywords(product_name: str) -> list:
        # Полное удаление слова "смартфон" в любом регистре
        name_lower = re.sub(r'\bсмартфон\b', '', product_name.lower(), flags=re.IGNORECASE)

        # Расширенный список стоп-слов
        extended_stop_words = {
            'смартфон', 'smartfon', 'smartphone', 'phone', 'телефон',
            'mobile', 'мобильный', 'сотовый', 'смартфоны'
        }

        # Разбиваем на слова
        words = name_lower.split()

        # Список для финальных ключевых слов
        final_keywords = []

        for word in words:
            # Разбор памяти с разделителем "/"
            if '/' in word:
                memory_parts = word.split('/')
                final_keywords.extend([f"{part}gb" for part in memory_parts])
            else:
                # Фильтрация обычных слов
                if (
                        word not in extended_stop_words and
                        len(word) > 2 and
                        not word.isdigit() and
                        not re.match(r'\d+gb', word)
                ):
                    final_keywords.append(word)

        return list(set(final_keywords))


