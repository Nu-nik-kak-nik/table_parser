import csv
import os
from typing import List, Dict
from config import Config
from utils.logger import Logger

class OutputHandler:
    def __init__(self, output_path: str = None, logger: Logger = None):
        self.output_path = output_path or Config.OUTPUT_DICT
        self.logger = logger or Logger(__name__)

    def save_to_csv(self, data: List[Dict], filename: str = None):
        try:
            os.makedirs(self.output_path, exist_ok=True)
            filename = filename or f'matched_products_{self._get_timestamp()}.csv'
            full_path = os.path.join(self.output_path, filename)

            fieldnames = set()
            for item in data:
                fieldnames.update(item.keys())

            # Упорядочиваем столбцы: "Наше название", "Внешний код", затем пары "Цена X" и "Поставщик X"
            ordered_fieldnames = ['Наше название', 'Внешний код']
            price_columns = sorted([col for col in fieldnames if col.startswith('Цена')],
                                   key=lambda x: int(x.split()[1]))
            supplier_columns = sorted([col for col in fieldnames if col.startswith('Поставщик')],
                                      key=lambda x: int(x.split()[1]))

            for price_col, supplier_col in zip(price_columns, supplier_columns):
                ordered_fieldnames.extend([price_col, supplier_col])

            with open(full_path, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=ordered_fieldnames)
                writer.writeheader()
                writer.writerows(data)

            self.logger.info(f"Данные сохранены в {full_path}")
            return full_path

        except Exception as e:
            self.logger.error(f"Ошибка при сохранении CSV: {e}")
            raise

    @staticmethod
    def _get_timestamp():
        from datetime import datetime
        return datetime.now().strftime("%Y-%m-%d_%H:%M:%S")
