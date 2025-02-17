# from google.oauth2.service_account import Credentials
# import gspread
# import pandas as pd
# from typing import List, Dict
#
#
# class GoogleSheetHandler:
#     def __init__(self, sheet_url: str, credentials_path: str = 'credentials.json'):
#         """
#         Инициализация обработчика Google Sheets с современными библиотеками
#
#         :param sheet_url: URL Google Sheets
#         :param credentials_path: Путь к файлу credentials
#         """
#         try:
#             # Области доступа для Google Sheets
#             SCOPES = [
#                 'https://www.googleapis.com/auth/spreadsheets',
#                 'https://www.googleapis.com/auth/drive'
#             ]
#
#             # Загрузка credentials с использованием google-auth
#             creds = Credentials.from_service_account_file(
#                 credentials_path,
#                 scopes=SCOPES
#             )
#
#             # Авторизация клиента
#             self.client = gspread.authorize(creds)
#
#             # Открытие таблицы
#             self.sheet = self.client.open_by_url(sheet_url)
#
#         except Exception as e:
#             print(f"Ошибка авторизации: {e}")
#             raise
#
#     def get_worksheet(self, worksheet_index: int = 0):
#         """
#         Получение листа по индексу
#
#         :param worksheet_index: Индекс листа
#         :return: Лист Google Sheets
#         """
#         return self.sheet.get_worksheet(worksheet_index)
#
#     def get_all_records(self, worksheet_index: int = 0) -> List[Dict]:
#         """
#         Получение всех записей с листа
#
#         :param worksheet_index: Индекс листа
#         :return: Список словарей с записями
#         """
#         worksheet = self.get_worksheet(worksheet_index)
#         return worksheet.get_all_records()
#
#     def get_products(self, worksheet_index: int = 0) -> List[Dict]:
#         """
#         Получение данных о товарах из Google Sheets
#
#         :param worksheet_index: Индекс листа
#         :return: Список словарей с товарами
#         """
#         try:
#             records = self.get_all_records(worksheet_index)
#
#             products = []
#             for row in records:
#                 product = {
#                     'name': row.get('Наименование', ''),
#                     'external_code': row.get('Внешний код', ''),
#                     'manufacturer': row.get('Производитель', ''),
#                     'model': row.get('Модель', ''),
#                     'color': row.get('Цвет', ''),
#                     'memory': row.get('Встроенная память', '')
#                 }
#                 products.append(product)
#
#             return products
#
#         except Exception as e:
#             print(f"Ошибка получения продуктов: {e}")
#             return []
#
#     def save_results(self, results: List[Dict], worksheet_index: int = 0):
#         """
#         Сохранение результатов в Google Sheets
#
#         :param results: Список результатов для сохранения
#         :param worksheet_index: Индекс листа для сохранения
#         """
#         try:
#             worksheet = self.get_worksheet(worksheet_index)
#
#             # Преобразование результатов в формат для записи
#             headers = list(results[0].keys())
#             values = [headers]
#
#             for result in results:
#                 row_values = [result.get(header, '') for header in headers]
#                 values.append(row_values)
#
#             # Очистка листа перед записью
#             worksheet.clear()
#
#             # Запись результатов
#             worksheet.update(values)
#
#         except Exception as e:
#             print(f"Ошибка сохранения результатов: {e}")
import re


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
                if 1000 <= price <= 200000:
                    return price
            except (ValueError, TypeError):
                continue

print(_extract_price('AirPods MAX (USB-C) Orange   52000'))