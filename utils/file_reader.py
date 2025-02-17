import csv
import pandas as pd
from utils.logger import Logger


class FileReader:
    def __init__(self):
        self.logger = Logger(__name__)

    @staticmethod
    def read_csv(file_path):
        """Чтение CSV-файла и возврат данных в виде списка словарей."""
        logger = Logger(__name__)
        try:
            with open(file_path, mode='r', encoding='utf-8') as file:
                reader = csv.DictReader(file)

                logger.info(f'метод read_csv в FileReader был выполнен корректно')
                return list(reader)

        except Exception as e:
            logger.error(f"Ошибка при чтении CSV-файла {file_path}: {e}")
            return []

    @staticmethod
    def read_excel(file_path, sheet_name=0):
        """Чтение Excel-файла и возврат данных в виде списка словарей."""
        logger = Logger(__name__)
        try:
            df = pd.read_excel(file_path, sheet_name=sheet_name)

            logger.info(f'метод read_excel в FileReader был выполнен корректно')
            return df.to_dict('records')

        except Exception as e:
            logger.error(f"Ошибка при чтении excel-файла {file_path}: {e}")
            return []