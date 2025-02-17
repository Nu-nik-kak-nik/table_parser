import json
import re
from typing import List

from utils.logger import Logger

from transliterate import translit


class DictionaryHandler:
    def __init__(self, file_path):
        self.file_path = file_path
        self.logger = Logger(__name__)
        self.dictionaries = self._load_dictionaries()

    def _load_dictionaries(self):
        """Загружает словарь из JSON-файла. Если файл пустой или отсутствует, возвращает пустой словарь."""
        try:
            with open(self.file_path, 'r', encoding='utf-8') as file:
                data = file.read()
                if not data:
                    self.logger.warning("Файл словаря пустой. Будет создан новый словарь.")
                    return {}
                return json.loads(data)

        except FileNotFoundError:
            self.logger.warning("Файл словаря не найден. Будет создан новый словарь.")
            return {}

        except json.JSONDecodeError:
            self.logger.error("Ошибка при чтении JSON-файла. Файл содержит некорректные данные.")
            return {}

    def save_dictionaries(self):
        """Сохраняет словарь в JSON-файл."""
        with open(self.file_path, 'w', encoding='utf-8') as file:
            json.dump(self.dictionaries, file, indent=4, ensure_ascii=False)

    @staticmethod
    def _clean_keywords(product_name: str) -> List[str]:
        """Очищает ключевые слова от лишних символов и извлекает содержимое скобок."""
        # Извлекаем содержимое скобок и удаляем сами скобки
        brackets_content = re.findall(r'\((.*?)\)', product_name)
        product_name = re.sub(r'\(.*?\)', '', product_name)

        # Очищаем основное название от лишних символов
        cleaned = re.sub(r'[(),]', ' ', product_name)
        keywords = [word.strip().lower() for word in cleaned.split() if word.strip()]

        # Добавляем содержимое скобок как отдельные ключевые слова
        for content in brackets_content:
            keywords.extend([word.strip().lower() for word in content.split() if word.strip()])

        return keywords

    def add_dictionary(self, product_name, dictionary):
        """Добавляет словарь для товара."""
        try:
            self.dictionaries[product_name] = dictionary
            self.save_dictionaries()
            self.logger.info(f"Словарь для товара '{product_name}' успешно добавлен.")

        except Exception as e:
            self.logger.error(f"Ошибка при добавлении словаря: {e}")

    @staticmethod
    def _add_transliterations(keywords: List[str]) -> List[str]:
        """
        Добавляет транслитерации и варианты написания для ключевых слов.
        Транслитерация только для iphone и ipad.
        """
        variations = set(keywords)

        for keyword in keywords:
            # Транслитерация только для iphone и ipad
            if keyword.lower() in {'iphone', 'ipad'}:
                transliterated = translit(keyword, 'ru', reversed=True)
                variations.add(transliterated)

        return list(variations)

    def get_dictionary(self, product_name: str) -> List[str]:
        """Возвращает словарь ключевых слов для товара."""
        if product_name not in self.dictionaries:
            # Очищаем ключевые слова
            keywords = self._clean_keywords(product_name)
            # Добавляем вариации (без полного названия товара)
            variations = set(keywords)
            # Добавляем транслитерации и варианты написания
            variations.update(self._add_transliterations(keywords))
            self.dictionaries[product_name] = list(variations)
            self.save_dictionaries()

        return self.dictionaries.get(product_name, [])

    def update_dictionary(self, product_name, new_keywords):
        """Обновляет словарь ключевых слов для товара."""
        if product_name in self.dictionaries:
            self.dictionaries[product_name].extend(new_keywords)
            self.dictionaries[product_name] = list(set(self.dictionaries[product_name]))  # Убираем дубликаты
            self.save_dictionaries()
            self.logger.info(f"Словарь для товара '{product_name}' успешно обновлен.")
        else:
            self.logger.warning(f"Товар '{product_name}' не найден в словаре.")

