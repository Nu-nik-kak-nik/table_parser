import csv
import os
import pandas as pd
from typing import List, Dict


class CSVHandler:
    def __init__(self, output_dir='output'):
        """
        Инициализация CSV обработчика

        :param output_dir: Директория для сохранения результатов
        """
        # Создание директории, если её нет
        os.makedirs(output_dir, exist_ok=True)
        self.output_dir = output_dir

    def read_csv(self, filepath: str, encoding: str = 'utf-8') -> List[Dict]:
        """
        Чтение CSV файла

        :param filepath: Путь к файлу
        :param encoding: Кодировка файла
        :return: Список словарей с данными
        """
        try:
            # Использование pandas для корректной работы с различными форматами
            df = pd.read_csv(filepath, encoding=encoding, dtype=str)
            return df.to_dict('records')

        except FileNotFoundError:
            print(f"Файл {filepath} не найден")
            return []

        except Exception as e:
            print(f"Ошибка при чтении CSV: {e}")
            return []

    def save_results(
            self,
            results: List[Dict],
            filename: str = 'matching_results.csv',
            columns_order: List[str] = None
    ):
        """
        Сохранение результатов в CSV файл

        :param results: Список результатов
        :param filename: Имя файла для сохранения
        :param columns_order: Порядок столбцов (опционально)
        """
        try:
            # Полный путь к файлу
            filepath = os.path.join(self.output_dir, filename)

            # Преобразование в DataFrame
            df = pd.DataFrame(results)

            # Упорядочивание столбцов, если указан порядок
            if columns_order:
                # Добавляем столбцы, которых нет в порядке
                existing_columns = [col for col in columns_order if col in df.columns]
                additional_columns = [col for col in df.columns if col not in columns_order]

                columns_order = existing_columns + additional_columns
                df = df[columns_order]

            # Сохранение с поддержкой кириллицы
            df.to_csv(filepath, encoding='utf-8', index=False, sep=';')

            print(f"Результаты сохранены в {filepath}")

        except Exception as e:
            print(f"Ошибка при сохранении CSV: {e}")

    def expand_suppliers_column(self, results: List[Dict]) -> List[Dict]:
        """
        Расширение колонки поставщиков для соответствия формату из примера

        :param results: Исходный список результатов
        :return: Список с развернутыми данными о поставщиках
        """
        expanded_results = []

        for result in results:
            # Базовая запись без поставщиков
            base_record = result.copy()
            base_record.pop('suppliers', None)

            # Если поставщики есть
            if result.get('suppliers'):
                # Добавляем до 3 поставщиков
                for i, supplier in enumerate(result['suppliers'][:3], 1):
                    record = base_record.copy()
                    record[f'Поставщик {i}'] = supplier.get('name', '')
                    record[f'Цена поставщика {i}'] = supplier.get('price', '')
                    expanded_results.append(record)
            else:
                # Если поставщиков нет
                expanded_results.append(base_record)

        return expanded_results
