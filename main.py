from utils.data_processor import DataProcessor
from utils.output_handler import OutputHandler
from config import Config
from utils.logger import Logger

def main():
    logger = Logger(__name__)
    logger.info("Запуск процесса сопоставления продуктов")

    try:
        processor = DataProcessor()
        matched_products = processor.process_data()

        if matched_products:
            output_handler = OutputHandler(Config.OUTPUT_DICT)
            output_handler.save_to_csv(matched_products)
            logger.info("Процесс сопоставления продуктов успешно завершен")
        else:
            logger.error("Не найдено соответствующих продуктов")
    except Exception as e:
        logger.error(f"Процесс сопоставления продуктов завершен неуспешно. Ошибка: {e}")

if __name__ == "__main__":
    main()