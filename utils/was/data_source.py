import pandas as pd
import os
from typing import List, Dict, Optional

from config import Config
from utils.logger import Logger


class CSVDataSource:
    def __init__(
            self,
            config: 'Config' = None,
            logger: Optional[Logger] = None
    ):
        """
        Инициализация источника данных

        :param config: Конфигурация
        :param logger: Логгер
        """
        # Импорт конфига внутри метода для избежания циклического импорта
        from config import Config

        self.config = config or Config()
        self.logger = logger or Logger(__name__)

        # Пути к файлам по умолчанию (можно настроить через конфиг)
        self.shop_products_path = getattr(
            self.config,
            'SHOP_PRODUCTS_PATH',
            'data/shop_products.csv'
        )
        self.supplier_products_path = getattr(
            self.config,
            'SUPPLIER_PRODUCTS_PATH',
            'data/supplier_products.csv'
        )

    def read_shop_products(self) -> List[Dict]:
        """
        Чтение продуктов магазина из CSV

        :return: Список словарей с продуктами
        """
        try:
            # Проверка существования файла
            if not os.path.exists(self.shop_products_path):
                self.logger.warning(f"Файл {self.shop_products_path} не найден")
                return []

            # Чтение CSV-файла
            df = pd.read_csv(
                self.shop_products_path,
                dtype=str,  # Преобразование всех столбцов в строки
                encoding='utf-8'
            )

            # Очистка и фильтрация данных
            df = self._clean_dataframe(df)

            # Преобразование в список словарей
            products = df.to_dict('records')

            self.logger.info(f"Загружено {len(products)} продуктов магазина")
            return products

        except Exception as e:
            self.logger.error(f"Ошибка при чтении продуктов магазина: {e}")
            return []

    def read_supplier_products(self) -> List[Dict]:
        """
        Чтение продуктов поставщиков из CSV

        :return: Список словарей с продуктами поставщиков
        """
        try:
            # Проверка существования файла
            if not os.path.exists(self.supplier_products_path):
                self.logger.warning(f"Файл {self.supplier_products_path} не найден")
                return []

            # Чтение CSV-файла
            df = pd.read_csv(
                self.supplier_products_path,
                dtype=str,  # Преобразование всех столбцов в строки
                encoding='utf-8'
            )

            # Очистка и фильтрация данных
            df = self._clean_dataframe(df)

            # Преобразование в список словарей
            products = df.to_dict('records')

            self.logger.info(f"Загружено {len(products)} продуктов поставщиков")
            return products

        except Exception as e:
            self.logger.error(f"Ошибка при чтении продуктов поставщиков: {e}")
            return []

    @staticmethod
    def _clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
        """
        Очистка и предобработка DataFrame

        :param df: Исходный DataFrame
        :return: Обработанный DataFrame
        """
        # Удаление пустых строк
        df.dropna(how='all', inplace=True)

        # Переименование столбцов (если требуется)
        column_mapping = {
            'Наименование': 'Название',
            'Код товара': 'Код',
            'Цена': 'Цена'
        }
        df.rename(columns=column_mapping, inplace=True)

        # Очистка значений
        for column in df.columns:
            # Преобразование к строке и удаление лишних пробелов
            df[column] = df[column].astype(str).str.strip()

        # Фильтрация пустых значений
        df = df[df['Название'] != '']

        return df
