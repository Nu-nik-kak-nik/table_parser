import logging
import os

from config import Config


class Logger:
    def __init__(self, name, log_file=Config.LOG_FILE):
        """
        Инициализация логгера.

        :param name: Имя логгера (обычно __name__).
        :param log_file: Путь к файлу для записи логов.
        """
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.INFO)  # Уровень логирования INFO

        # Создаем папку для логов, если её нет
        log_dir = os.path.dirname(log_file)
        if log_dir and not os.path.exists(log_dir):
            os.makedirs(log_dir)

        # Формат сообщений
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

        # Обработчик для записи в файл
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)
        self.logger.addHandler(file_handler)

        # Обработчик для вывода в консоль
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        self.logger.addHandler(console_handler)

    def info(self, message):
        """Логирование информационных сообщений."""
        self.logger.info(message)

    def error(self, message):
        """Логирование ошибок."""
        self.logger.error(message)

    def warning(self, message):
        """Логирование предупреждений."""
        self.logger.warning(message)

    def debug(self, message):
        """Логирование отладочных сообщений."""
        self.logger.debug(message)