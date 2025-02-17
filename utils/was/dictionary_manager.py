import json
from typing import Dict, List, Any


class DictionaryManager:
    @staticmethod
    def load_json(filepath: str) -> Dict:
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                return json.load(f)
        except FileNotFoundError:
            return {}

    @staticmethod
    def save_json(data: Dict, filepath: str):
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

    @staticmethod
    def create_product_keywords(product: Dict) -> List[str]:
        """Генерация ключевых слов для товара"""
        keywords = []

        # Базовые атрибуты
        keywords.extend([
            product.get('Наименование', '').lower(),
            product.get('Производитель', '').lower(),
            product.get('Модель', '').lower(),
            product.get('Цвет', '').lower(),
        ])

        # Дополнительные характеристики
        memory = product.get('Встроенная память', '')
        if memory:
            keywords.extend([
                memory,
                f"{memory}gb",
                f"{memory} gb"
            ])

        # Эмодзи и сокращения
        if 'Apple' in product.get('Производитель', ''):
            keywords.append('🍏')

        return list(set(keywords))
