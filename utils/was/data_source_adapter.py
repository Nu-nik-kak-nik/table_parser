from abc import ABC, abstractmethod
import csv


class DataSourceAdapter(ABC):
    @abstractmethod
    def read_data(self, source):
        """Абстрактный метод чтения данных из различных источников"""
        pass


class CSVDataSource(DataSourceAdapter):
    def read_data(self, file_path):
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                return list(csv.DictReader(f))
        except FileNotFoundError:
            raise ValueError(f"Файл не найден: {file_path}")
        except Exception as e:
            raise ValueError(f"Ошибка чтения CSV: {e}")


class GoogleSheetsDataSource(DataSourceAdapter):
    def __init__(self, credentials):
        self.credentials = credentials

    def read_data(self, sheet_id, range_name):
        # Заглушка для будущей реализации
        # Здесь будет логика работы с Google Sheets
        pass
